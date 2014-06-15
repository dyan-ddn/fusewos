/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>
  Copyright (C) 2011       Sebastian Pipping <sebastian@pipping.org>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.

  gcc -Wall fusexmp.c `pkg-config fuse --cflags --libs` -o fusexmp
*/

#define FUSE_USE_VERSION 26

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef linux
/* For pread()/pwrite()/utimensat() */
#define _XOPEN_SOURCE 700
#endif

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

#include <cstdio>
#include <cstdlib>
#include <stddef.h>
#include <pthread.h>
#include <vector>
#include <wos_cluster.hpp>
#include <wos_obj.hpp>

#include <syslog.h>
#include<pthread.h>

using namespace wosapi;

/**
 ** A simple client that demonstrates blocking reads and writes.
 **/
class BlockWOSClient
{
public:
   WosClusterPtr wos;

   //BlockWOSClient(std::string cloud);
   BlockWOSClient();

   WosPolicy GetPolicy(std::string name);

   WosObjPtr Get(WosOID oid);

   int Connect(std::string cloud);

};

BlockWOSClient::BlockWOSClient()
{
}

int
BlockWOSClient::Connect(std::string cloud)
{  
   wos = WosCluster::Connect(cloud);
   return 1;
}

WosPolicy
BlockWOSClient::GetPolicy(std::string name)
{
   return wos->GetPolicy(name);
}

WosObjPtr
BlockWOSClient::Get(WosOID oid)
{
   WosStatus s;
   WosObjPtr o;
   wos->Get(s, oid, o);
   return o;
}

const char* host = "10.44.34.73";
BlockWOSClient wos_b;

/** 
 **  struct wosclient_pool_entry and helpers 
 **/
typedef struct { //type union can not be used with WosPutStreamPtr and etc
	WosPutStreamPtr ps;
	WosGetStreamPtr gs;
	WosObjPtr w;
 	uint64_t put_bytes;
	uint64_t get_bytes;;	
} WosPtr_t;

#define WOS_READ	1
#define WOS_WRITE	2
struct wosclient_pool_entry {
	int type;
	uint64_t len;
	WosPtr_t WosPtr;
	const char *path;
	struct wosclient_pool_entry *next;
} *wosclient_pool_head, *wosclient_pool_curr;
int wosclient_pool_count =0;
int wosclient_pool_empty_count = 0;

struct wosobj_info {
	char magic[7];
	char oid[41];
	uint64_t obj_len;
	time_t sec;		
};

#define WOSFS_VERSION "0.2.2"
#define WOS_MAGIC "DDNWOS"
#define WOS_DEFAULT_PATH "/wosfs/"
#define WOS_DEFAULT_IP "10.44.34.73"
#define WOS_DEFAULT_POLICY "test"
#define WOSFS_PATH_FIX_01 0

char wos_fs_path[256];
char wos_ip[256];
char wos_policy[256];
char wosfs_default_magic[]=WOS_MAGIC;
char wosfs_default_path[]=WOS_DEFAULT_PATH;
char wos_default_ip[]=WOS_DEFAULT_IP;
char wos_default_policy[]=WOS_DEFAULT_POLICY;

pthread_mutex_t lock;
pthread_mutex_t lock2;
pthread_mutex_t lock_release;
pthread_mutex_t lock_write;;
pthread_mutex_t lock_read;;
pthread_mutex_t lock_getattr;


struct wosfs_config {
     char *wosfs_magic;
     char *wosfs_path;
     char *wosfs_bak_path;
     char *wos_ip;
     char *wos_policy;
     int   wosfs_debug;
} wosfs_conf;;

enum {
     KEY_HELP,
     KEY_VERSION,
};

#define WOSFS_OPT(t, p, v) { t, offsetof(struct wosfs_config, p), v }

#define WOSFS_LOG_ERRORS	0x01  // bitmask for error output
#define WOSFS_LOG_FILEOP	0x02  // bitmask for fileop debug output
static struct fuse_opt wosfs_opts[] = {
     WOSFS_OPT("-m %s",             wosfs_magic, 0),
     WOSFS_OPT("-l %s",		    wosfs_path, 0),
     WOSFS_OPT("-b %s",       	    wosfs_bak_path, 0),
     WOSFS_OPT("-w %s",       	    wos_ip, 0),
     WOSFS_OPT("-p %s",       	    wos_policy, 0),
     WOSFS_OPT("--wosdebug=%i",     wosfs_debug, 0),

     FUSE_OPT_KEY("-V",             KEY_VERSION),
     FUSE_OPT_KEY("--version",      KEY_VERSION),
     FUSE_OPT_KEY("-h",             KEY_HELP),
     FUSE_OPT_KEY("--help",         KEY_HELP),
     FUSE_OPT_END
};

#define WOSFS_DEBUGLOG(m, fmt, args ...) { if (wosfs_conf.wosfs_debug & m ) syslog(LOG_INFO, "%s:%u:%s()" fmt, __FILE__, __LINE__, __func__, ## args);}
	
bool test_wos_magic(FILE *fp)
{
	char magic[7];

	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN");

	rewind(fp);
	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: Check 100");
        fread((void *)magic, 1, 6, fp); magic[6]=0;
	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: Check 101");
	rewind(fp);

	if (memcmp(magic, WOS_MAGIC, sizeof(WOS_MAGIC)) == 0 )	
		return true;
	else
		return false;
}

bool wosobj_info_last(const char *path, struct wosobj_info *wosobj_info)
{  
	bool res=false; 
        FILE *fp;
        fp = fopen(path, "r+");

        if ( NULL == fp) {
		WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS:: failed to open file %s", path);
                return res;
        }

	ssize_t read;
	char *line = NULL;
	char *lastline = NULL;
	size_t len = 0;
	while ((read = getline(&line, &len, fp)) != -1) {
		lastline = line;
		//printf("Retrieved line of length %zu :\n", read);
		//printf("%s", lastline);
		
        }
	if (lastline) {
		sscanf(lastline,  "%s %s %lu", wosobj_info->magic, wosobj_info->oid, &wosobj_info->obj_len);
	 	res = true;	
	}
	else
		WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: file %s is empty", path);

	fclose(fp);

	return res;
}

struct wosclient_pool_entry* wosclient_pool_create_list(const char *path, WosPtr_t WosPtr)
{
    struct wosclient_pool_entry *ptr = (struct wosclient_pool_entry*)malloc(sizeof(struct wosclient_pool_entry));

    wosclient_pool_empty_count++;
    if ( NULL == ptr ) {
	WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS:: failed to allocate memory for ptr");
	return NULL;
    }
    // must initialize to 0, otherwise strict check will complain and generate segment faults.
    memset(ptr, 0, sizeof(wosclient_pool_entry));

    ptr->WosPtr = WosPtr;
    ptr->path = (char *)malloc(strlen(path)+1);
    if ( NULL == ptr->path ) {
        WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS:: failed to allocate memory for ptr->path");
        return NULL;
    }
    strcpy((char *)ptr->path, path);
    ptr->next = NULL;

    wosclient_pool_head = wosclient_pool_curr = ptr;
    WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: OUT: wosclient_pool_empty_count=%d", wosclient_pool_empty_count);
    wosclient_pool_count++;
    return ptr;
}

struct wosclient_pool_entry* wosclient_pool_add_to_list(const char *path, WosPtr_t WosPtr, bool add_to_end)
{

    pthread_mutex_lock(&lock);
    WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: path=%s", path);

    if(NULL == wosclient_pool_head)
    {
    	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: wosclient_pool is empty");
	pthread_mutex_unlock(&lock);
        return (wosclient_pool_create_list(path, WosPtr));
    }

    struct wosclient_pool_entry *ptr = (struct wosclient_pool_entry*)malloc(sizeof(struct wosclient_pool_entry));
    if ( NULL == ptr ) {
        WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS:: failed to allocate memory for ptr");
	pthread_mutex_unlock(&lock);
        return NULL;
    }  
    memset(ptr, 0, sizeof(wosclient_pool_entry));

    ptr->WosPtr = WosPtr;
    ptr->path = (char *)malloc(strlen(path)+1);
    if ( NULL == ptr->path ) {
	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: failed to allocate memory for ptr->path");
	pthread_mutex_unlock(&lock);
	return NULL;
    }
    strcpy((char *)ptr->path, path);
    ptr->next = NULL;

    if(add_to_end)
    {
        wosclient_pool_curr->next = ptr;
        wosclient_pool_curr = ptr;
    }
    else
    {
        ptr->next = wosclient_pool_head;
        wosclient_pool_head = ptr;
    }

    wosclient_pool_count++;

    pthread_mutex_unlock(&lock);
    return ptr;
}

struct wosclient_pool_entry* wosclient_pool_search_in_list_by_path(const char *path, struct wosclient_pool_entry **prev)
{
    struct wosclient_pool_entry *ptr = wosclient_pool_head;
    struct wosclient_pool_entry *tmp = NULL;
    bool found = false;

    while(ptr != NULL)
    {
        if(strcmp(ptr->path,path) == 0)
        {
            found = true;
            break;
        }
        else
        {
            tmp = ptr;
            ptr = ptr->next;
        }
    }

    if(true == found)
    {
        if(prev)
            *prev = tmp;
        return ptr;
    }
    else
    {
        return NULL;
    }
}

int wosclient_pool_delete_call_count=0;
int wosclient_pool_delete_from_list(const char *path)
{
    struct wosclient_pool_entry *prev = NULL;
    struct wosclient_pool_entry *del = NULL;

    pthread_mutex_lock(&lock);

    WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: path=%s, calls=%d, cur number: %d", path, ++wosclient_pool_delete_call_count, wosclient_pool_count);
    del = wosclient_pool_search_in_list_by_path(path,&prev);
    if(del == NULL)
    {
	pthread_mutex_unlock(&lock);
        return -1;
    }
    else
    {
        if(prev != NULL)
            prev->next = del->next;

        if(del == wosclient_pool_curr)
        {
            wosclient_pool_curr = prev;
        }

        if(del == wosclient_pool_head)
        {
            wosclient_pool_head = del->next;
        }
    }

    free((void *)del->path);
    free(del);
    del = NULL;
    wosclient_pool_count--;

    pthread_mutex_unlock(&lock);

    return 0;
}

void wosclient_pool_print_list(void)
{
    struct wosclient_pool_entry *ptr = wosclient_pool_head;

    WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, " -------Printing list Start------- ");
    while(ptr != NULL)
    {
        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, "[%s]",ptr->path);
        ptr = ptr->next;
    }
    WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, " -------Printing list End------- ");

    return;
}


WosOID store_obj_b(BlockWOSClient *wos_b, const char *pdata, size_t len)
{
	WosClusterPtr wos = wos_b->wos;	
	WosPolicy policy = wos->GetPolicy(wosfs_conf.wos_policy);

	// create an object; associate data, metadata with it 
	WosObjPtr obj = WosObj::Create();

	obj->SetData(pdata, len); 

	WosStatus rstatus; // return status 
	WosOID roid;// return oid
	wos->Put(rstatus, roid, policy, obj);
	if (rstatus != ok) {
		WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS:: Error during Put: %s", rstatus.ErrMsg().c_str());
		return false; 
	}

	return roid;
}

bool store_obj_stream_b(WosPutStreamPtr wosps, const char *pdata, size_t len, off_t offset)
{
	WosStatus rstatus;

	wosps->PutSpan(rstatus, pdata, offset, len);	
	if (rstatus != ok) {
		WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS:: Error in PutSpan %d", offset); 
	}
}

/* 
 *  Helpers
 */

char wosfs_ns_paths[64][65];
int  wosfs_ns_paths_depth = 0;
bool wosfs_parse_ns_paths(char *path)  //path need to be a full path string starting with "/"
{
	char *path2 = path;

	if ( path[0] != '/' )
		return false;

	int i, j;

	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: sizeof(wosfs_ns_paths)=%d", sizeof(wosfs_ns_paths));
	memset(wosfs_ns_paths, 0, sizeof(wosfs_ns_paths));

	i=0;
	wosfs_ns_paths[i][0] = '/';
	path2++;
	i++;
	do {
		WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: path2[0] = %c", path2[0]);
		if (path2[0] == '/') {
			char *p = wosfs_ns_paths[i];
			strncpy(p, path, path2-path);
			i++;
			if (i > 64 )
				return false;
		}
		path2++;
	} while (path2[0] != (char)NULL);
	wosfs_ns_paths_depth = i;

	for (j = 0; j< i; j++)
		WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: wosfs_ns_path[%d] = %s", j, wosfs_ns_paths[j]);

	return true;

}
bool wosfs_match_ns(const char *path)
{

	int i;

	for (i=0; i< wosfs_ns_paths_depth; i++)
		if (strcmp(path, wosfs_ns_paths[i]) == 0 )
			return true;
		
	return false;	
}

void wosfs_fix_path(const char *path, char *path2)  //path2 need to be pre-malloc'ed and init with 0
{
        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s", path);

        strcpy(path2, wosfs_conf.wosfs_path);
        strcat(path2, path);

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: OUT : path=%s, path2=%s", path, path2);

}
/* 
 *  wosfs_xxx functions
 */

static int wosfs_getattr(const char *path, struct stat *stbuf)
{
	int res = -ENOENT;

	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s, size=%d", path, stbuf->st_size)

	char *path2=(char *)malloc(256);;
	memset((void *)path2, 0, 256);
	
	wosfs_fix_path(path, path2);

        res = lstat(path2, stbuf);
        if (res == -1) {
		free(path2);
		return -errno;
	}

        if (S_ISREG(stbuf->st_mode)) {
                struct wosobj_info wosobj_info;
                memset(&wosobj_info, 0, sizeof(struct wosobj_info));
                if ( wosobj_info_last(path2, &wosobj_info) == true )
	                stbuf->st_size = wosobj_info.obj_len;
        }

	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: OUT : path2=%s, size=%d, res=%d", path2, stbuf->st_size, res);
	free(path2);

	return res;
}

static int wosfs_access(const char *path, int mask)
{
	int res = -ENOENT;

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s", path);

        char *path2=(char *)malloc(256);
        memset((void *)path2, 0, 256);

     	wosfs_fix_path(path, path2);   
	res = access(path2, mask);
	if (res == -1) {
		free(path2);
		return -errno;
	}

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: OUT : path2=%s, res=%d", path2, res);
	free(path2);

	return res;
}

static int wosfs_readlink(const char *path, char *buf, size_t size)
{
	int res = -ENOENT;

	memset(buf, 0, size-1);
        char *path2=(char *)malloc(256);
        memset((void *)path2, 0, 256);

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s, size=%d", path, size);
        wosfs_fix_path(path, path2);
        res = readlink(path2, buf, size - 1);
        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: path2=%s, buf=%s, size-1=%d, res=%d", path2, buf, size-1, res);
        if (res == -1) {
		free(path2);
                return -errno;
	}

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: OUT : path2=%s, res=%d, sizeof(path2)", path2, res);
	free(path2);

        return 0;
}

static int wosfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		       off_t offset, struct fuse_file_info *fi)
{
	int res = -ENOENT;

        char *path2=(char *)malloc(256);
        memset((void *)path2, 0, 256);

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s", path);
        wosfs_fix_path(path, path2);

        DIR *dp;
        struct dirent *de;

        (void) offset;
        (void) fi;

        dp = opendir(path2);
        if (dp == NULL) {
		free(path2);
                return -errno;
	}

        while ((de = readdir(dp)) != NULL) {
                struct stat st;
                memset(&st, 0, sizeof(st));
                st.st_ino = de->d_ino;
                st.st_mode = de->d_type << 12;
                if (filler(buf, de->d_name, &st, 0))
                        break;
        }

        closedir(dp);
	free(path2);

        return 0;
}

static int wosfs_mknod(const char *path, mode_t mode, dev_t rdev)
{
	int res = -ENOENT;

        char path2[256];
        memset((void *)&path2, 0, 256);

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s", path);
        wosfs_fix_path(path, path2);

        /* On Linux this could just be 'mknod(path, mode, rdev)' but this
           is more portable */
        if (S_ISREG(mode)) {
                res = open(path2, O_CREAT | O_EXCL | O_WRONLY, mode);
                if (res >= 0)
                        res = close(res);
        } else if (S_ISFIFO(mode))
                res = mkfifo(path2, mode);
        else   
                res = mknod(path2, mode, rdev);
        if (res == -1)
                return -errno;

        return 0;
}

static int wosfs_mkdir(const char *path, mode_t mode)
{
	int res = -ENOENT;

        char path2[256];
        memset((void *)&path2, 0, 256);

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s", path);
        wosfs_fix_path(path, path2);

        res = mkdir(path2, mode);
        if (res == -1)
                return -errno;

        if ( wosfs_conf.wosfs_bak_path ) {
                int res2;
                char tgt_path[256];
                memset(tgt_path, 0, 256);
                int i;

                i=strlen(wosfs_conf.wosfs_bak_path);
                strncpy(tgt_path, wosfs_conf.wosfs_bak_path, i);
                strncpy(tgt_path+i, path+strlen(wosfs_conf.wosfs_path), strlen(path)-strlen(wosfs_conf.wosfs_path));
                WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: OUT : make new directory in backup path: path=%s, tgt_path=%s", path, tgt_path);

                res2 = mkdir(tgt_path, mode);
                if (res == -1) {
                        WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS:: OUT : failed to make directory path=%s", tgt_path);
                }
        }

        return 0;
}

static int wosfs_unlink(const char *path)
{
	int res = -ENOENT;

        char path2[256];
        memset((void *)&path2, 0, 256);

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s", path);
        wosfs_fix_path(path, path2);

        struct stat stbuf;

        res = lstat(path2, &stbuf);
        if (res == -1)
                return -errno;

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s", path);

        if (S_ISREG(stbuf.st_mode)) {

                struct wosobj_info wosobj_info;
                memset(&wosobj_info, 0, sizeof(struct wosobj_info));
                if ( wosobj_info_last(path2, &wosobj_info) == false) {
                        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: failed to read from file: %s", path2);
                }
                else if ( stbuf.st_nlink == 1 ) {
                        WosStatus status;
                        WosOID oid(wosobj_info.oid);
                        wos_b.wos->Delete(status, oid);
                        if (status != ok) {
                                WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: WOS delete status=%s", status.ErrMsg().c_str());
                        }
                        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: path=%s, oid=%s", path2, wosobj_info.oid);
                }
        }
        res = unlink(path2);
        if (res == -1)
                return -errno;
}

static int wosfs_rmdir(const char *path)
{
	int res = -ENOENT;

        char path2[256];
        memset((void *)&path2, 0, 256);

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s", path);
        wosfs_fix_path(path, path2);

        res = rmdir(path2);
        if (res == -1)
                return -errno;

        return 0;
}

static int wosfs_symlink(const char *from, const char *to)
{
	int res = -ENOENT;

	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: from=%s, to=%s", from, to);

        char from2[256];
        char to2[256];
        memset((void *)&to2, 0, 256);

        wosfs_fix_path(to, to2);
	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: from=%s, to2=%s", from, to2);

        res = symlink(from, to2);
        if (res == -1) {
                WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS:: failed to make link from from=%s to to=%s", from, to);
                return -errno;
        }
        
        return res;
}

static int wosfs_rename(const char *from, const char *to)
{
	int res = -ENOENT;

	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : from=%s, to=%s", from, to);

        char from2[256];
        char to2[256];
        memset((void *)&from2, 0, 256);
        memset((void *)&to2, 0, 256);

        wosfs_fix_path(from, from2);
        wosfs_fix_path(to, to2);

        res = rename(from2, to2);
        if (res == -1)
                return -errno;

        return 0;
}

static int wosfs_link(const char *from, const char *to)
{
	int res = -ENOENT;

        char from2[256];
        char to2[256];
        memset((void *)&from2, 0, 256);
        memset((void *)&to2, 0, 256);

	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : from=%s, to=%s", from, to);

        wosfs_fix_path(from, from2);
        wosfs_fix_path(to, to2);

        res = link(from2, to2);
        if (res == -1)
                return -errno;

        return 0;
}

static int wosfs_chmod(const char *path, mode_t mode)
{
	int res = -ENOENT;

        char path2[256];
        memset((void *)&path2, 0, 256);

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s", path);
        wosfs_fix_path(path, path2);

        res = chmod(path2, mode);
        if (res == -1)
                return -errno;

        return 0;
}

static int wosfs_chown(const char *path, uid_t uid, gid_t gid)
{
	int res = -ENOENT;

        char path2[256];
        memset((void *)&path2, 0, 256);

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s", path);
        wosfs_fix_path(path, path2);

        res = lchown(path2, uid, gid);
        if (res == -1)  
                return -errno;

        return 0;
}

static int wosfs_truncate(const char *path, off_t size)
{
	int res = -ENOENT;

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s", path);

      	if (strncmp (path, wosfs_conf.wosfs_path, strlen(wosfs_conf.wosfs_path)) == 0) {

        struct stat stbuf;

        res = lstat(path, &stbuf);
        if (res == -1)
                return -errno;

      	if (strncmp (path, wosfs_conf.wosfs_path, strlen(wosfs_conf.wosfs_path)) == 0) {
		/* let us to trucate the file if needed */
                return 0;
        }

	res = truncate(path, size);
	if (res == -1)
		return -errno;

	return 0;
	}
	return res;
}

static int wosfs_utimens(const char *path, const struct timespec ts[2])
{
	int res = -ENOENT;

        char path2[256];
        memset((void *)&path2, 0, 256);

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s", path);
        wosfs_fix_path(path, path2);

        /* don't use utime/utimes since they follow symlinks */
        res = utimensat(0, path2, ts, AT_SYMLINK_NOFOLLOW);
        if (res == -1)
                return -errno;

        return 0;
}

static int wosfs_open(const char *path, struct fuse_file_info *fi)
{
	int res = -ENOENT;

        char path2[256];
        memset((void *)&path2, 0, 256);

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s", path);
        wosfs_fix_path(path, path2);

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS: IN : path=%s", path);
        res = open(path2, fi->flags);
        if (res == -1)
                return -errno;

        close(res);

        return 0;
}

static int wosfs_read(const char *path1, char *buf, size_t size, off_t offset,
		    struct fuse_file_info *fi)
{
	int res = -ENOENT;
	char *path = (char *)path1;

	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS: IN : path=%s, offset = %u, size = %u", path, offset, size); 

	(void)fi;

        struct stat stbuf;

        char path2[256];
        memset((void *)&path2, 0, 256);

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s", path);
        wosfs_fix_path(path, path2);

	path=path2;	

        res = lstat(path, &stbuf);
        if (res == -1)
                return -errno;

	if (S_ISREG(stbuf.st_mode)) {

	//pthread_mutex_lock(&lock_read);
        struct wosclient_pool_entry *wosclient = NULL;

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: path=%s, Check Point 1000", path);
        wosclient = wosclient_pool_search_in_list_by_path(path, NULL);
        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: path=%s, Check Point 1001", path);
        if ( NULL == wosclient )        {
                WosPtr_t WosPtr;
                WosPtr.get_bytes=0;

                char oid_str[41];
                struct wosobj_info wosobj_info;
                memset(&wosobj_info, 0, sizeof(struct wosobj_info));
        	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: path=%s, Check Point 1002", path);
                if ( wosobj_info_last(path, &wosobj_info) == false ) {
			WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS:: OUT : can not read stub file: path=%s", path);
			//pthread_mutex_unlock(&lock_read);
			//return -errno;
			return EAGAIN;
		}

        	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: path=%s, Check Point 1003", path);
                WosPtr.get_bytes = wosobj_info.obj_len;
		WosPtr.put_bytes = 0;

                WosOID oid(wosobj_info.oid);

                try {  
                        WosPtr.gs = wos_b.wos->CreateGetStream(oid);
                }
                catch (WosE_ObjectNotFound& e) {
                        WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS:: OUT : path=%s, Invalid OID: %s", path, oid.c_str());
			//pthread_mutex_unlock(&lock_read);
                        return -errno;
                }
        	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: path=%s, Check Point 1004", path);
                wosclient = wosclient_pool_add_to_list(path,WosPtr,true);
        	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: path=%s, Check Point 1005", path);
		wosclient->type = WOS_READ;
		wosclient->len = WosPtr.get_bytes;
		
        }
	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: path=%s, offset=%d, size=%d, WosPtr.get_bytes=%d, length=%d", path, offset, size, wosclient->WosPtr.get_bytes, wosclient->len);
	if ( wosclient->len > 0 ) {
        	WosStatus rstatus;
		WosObjPtr robj;

		if ( offset > wosclient->len-1 ) {
			WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS:: OUT : Invalid offset: offset=%d, length=%d", offset, wosclient->len);
			//pthread_mutex_unlock(&lock_read);
		 	return 0;  // can not return -errno as it will break some app like md5sum which will read beyond end of file.
		}

		if ( size > wosclient->len ) 
			size = wosclient->len;

		if ( (offset + size) > wosclient->len ) 
			size = wosclient->len - offset;

       		 wosclient->WosPtr.gs->GetSpan(rstatus, robj, offset, size);
		if (rstatus == ok) {
			const void* p;
			uint64_t objlen; 
			robj->GetData(p, objlen);
			memcpy(buf, p, objlen); 
			wosclient->WosPtr.get_bytes -= objlen;
			res = objlen;
		}
		else
			res = -errno;
	}
	}

	//pthread_mutex_unlock(&lock_read);
	return res;

}

static int wosfs_write(const char *path1, const char *buf, size_t size,
		     off_t offset, struct fuse_file_info *fi)
{
	int res = -ENOENT;
	char *path= (char *)path1;


        struct stat stbuf;

        char path2[256];
        memset((void *)&path2, 0, 256);

        wosfs_fix_path(path, path2);
        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path2=%s", path2);

	path = path2;

	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS: IN : path=%s, offset = %u, size = %u", path, offset, size); 

        res = lstat(path, &stbuf);
        if (res == -1)
                return -errno;

	//pthread_mutex_lock(&lock_write);
	if (S_ISREG(stbuf.st_mode)) {
        struct wosclient_pool_entry *wosclient = NULL; 

        wosclient = wosclient_pool_search_in_list_by_path(path, NULL);
        if ( NULL == wosclient )        {
		if ( offset != 0 ) {
			WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS::  not writing from BOF: path=%s, offset=%u, size=%u", path, offset, size);
			//pthread_mutex_unlock(&lock_write);
			return -errno;
		}

                WosPtr_t WosPtr;
                WosPtr.put_bytes = 0;
		WosPtr.get_bytes = 0;
		WosPolicy policy = wos_b.wos->GetPolicy(wosfs_conf.wos_policy);

                try {  
                	WosPtr.ps = wos_b.wos->CreatePutStream(policy);
                }
                catch (WosE_InvalidPolicy& e) {
                        WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS:: path=%s, Invalid Policy: %s", path, wosfs_conf.wos_policy);
			//pthread_mutex_unlock(&lock_write);
                        return -errno;
                }
                wosclient = wosclient_pool_add_to_list(path,WosPtr,true);
		wosclient->type = WOS_WRITE;
        }

	WosStatus rstatus;
	wosclient->WosPtr.ps->PutSpan(rstatus, (char *)buf, offset, size);
	if (rstatus != ok) {
		WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS: path=%s, Error in PutSpan at offset = %u with size = %u", path, offset, size); 
		//pthread_mutex_unlock(&lock_write);
		return -errno;
	}
	wosclient->WosPtr.put_bytes += size;
	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS: OUT: path=%s, offset = %u, size = %u, put_bytes= %u", path, offset, size, wosclient->WosPtr.put_bytes); 
	res=size;
	}

	//pthread_mutex_unlock(&lock_write);
	return res;
}

static int wosfs_statfs(const char *path, struct statvfs *stbuf)
{
	int res = -ENOENT;

        char path2[256];
        memset((void *)&path2, 0, 256);

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s", path);
        wosfs_fix_path(path, path2);

        res = statvfs(path2, stbuf);
        if (res == -1)
    	 	return -errno;
}

static int wosfs_release(const char *path1, struct fuse_file_info *fi)
{

	int res = -ENOENT;
        struct stat stbuf;
	char *path= (char *)path1;

	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s, cur number=%d", path, wosclient_pool_count);

        char path2[256];
        memset((void *)&path2, 0, 256);

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s", path);
        wosfs_fix_path(path, path2);
	
	path = path2;

        res = lstat(path, &stbuf);
        if (res == -1)
                return -errno;

	//pthread_mutex_lock(&lock_release);

        struct wosclient_pool_entry *wosclient = NULL;

        wosclient = wosclient_pool_search_in_list_by_path(path, NULL);
        if ( NULL == wosclient )        {
		//pthread_mutex_unlock(&lock_release);
                WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS:: OUT : no active wosclient found, path=%s", path);
                return res;
        }

	if ( wosclient->type == WOS_READ ) {
		wosclient_pool_delete_from_list(path);
		//pthread_mutex_unlock(&lock_release);
                WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS:: OUT : called with active WOS_READ stream?! path=%s", path);
		return res;
	}

	if ( (wosclient->type == WOS_WRITE) && (wosclient->WosPtr.put_bytes == 0)) {
		wosclient_pool_delete_from_list(path);
		//pthread_mutex_unlock(&lock_release);
                WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS:: OUT : called with active WOS_WRITE stream, but put_bytes=%d, path=%s", wosclient->WosPtr.put_bytes, path);
		return res;
	}

	WosStatus rstatus;
	WosOID roid; 
	uint64_t put_bytes = wosclient->WosPtr.put_bytes;
	WosPutStreamPtr ps = wosclient->WosPtr.ps;

	wosclient_pool_delete_from_list(path);
	ps->Close(rstatus, roid);

        if (rstatus != ok) {
                WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS:: OUT : Error in closing PutSteam %d");
		//pthread_mutex_unlock(&lock_release);
		return -errno;
        }

        const char *oid_str= roid.c_str();

        FILE * fp;
	time_t sec;
	sec = time (NULL);

	fp = fopen (path, "a+");
	if ( NULL == fp ) {
		WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS:: OUT : failed to open file.  path=%s", path);
		return -errno;
	}
	
	fprintf(fp, "%s %s %lu %ld %s %s\n", wosfs_conf.wosfs_magic, oid_str, put_bytes, sec, wosfs_conf.wos_ip, wosfs_conf.wos_policy);
   	fclose(fp);

	if ( wosfs_conf.wosfs_bak_path ) {
		char tgt_path[256];
		memset(tgt_path, 0, 256);
		int i;

		i=strlen(wosfs_conf.wosfs_bak_path);
		strncpy(tgt_path, wosfs_conf.wosfs_bak_path, i); 
		strncpy(tgt_path+i, path+strlen(wosfs_conf.wosfs_path), strlen(path)-strlen(wosfs_conf.wosfs_path));
		WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: OUT : write to backup path: orig path=%s, tgt_path=%s", path, tgt_path);

		fp = fopen (tgt_path, "a+");
		if ( NULL == fp ) {
	                WOSFS_DEBUGLOG(WOSFS_LOG_ERRORS, ":WOS:: OUT : failed to open file.  path=%s", path);
		}
		else {		
			fprintf(fp, "%s %s %lu %ld %s %s\n", wosfs_conf.wosfs_magic, oid_str, put_bytes, sec, wosfs_conf.wos_ip, wosfs_conf.wos_policy);
        		fclose(fp);
		}
	}	
	
	//pthread_mutex_unlock(&lock_release);
	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: OUT : path=%s, cur number=%d", path, wosclient_pool_count);
	return res;
}

static int wosfs_fsync(const char *path, int isdatasync,
		     struct fuse_file_info *fi)
{
	int res = -ENOENT;

        WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: IN : path=%s", path);

	/* Just a stub.	 This method is optional and can safely be left
	   unimplemented */

	(void) path;
	(void) isdatasync;
	(void) fi;
	return 0;
}

#ifdef HAVE_POSIX_FALLOCATE
static int wosfs_fallocate(const char *path, int mode,
			off_t offset, off_t length, struct fuse_file_info *fi)
{
	int fd;
	int res;

	(void) fi;

	if (mode)
		return -EOPNOTSUPP;

	fd = open(path, O_WRONLY);
	if (fd == -1)
		return -errno;

	res = -posix_fallocate(fd, offset, length);

	close(fd);
	return res;
}
#endif

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int wosfs_setxattr(const char *path, const char *name, const char *value,
			size_t size, int flags)
{
	int res = lsetxattr(path, name, value, size, flags);
	if (res == -1)
		return -errno;
	return 0;
}

static int wosfs_getxattr(const char *path, const char *name, char *value,
			size_t size)
{
	int res = lgetxattr(path, name, value, size);
	if (res == -1)
		return -errno;
	return res;
}

static int wosfs_listxattr(const char *path, char *list, size_t size)
{
	int res = llistxattr(path, list, size);
	if (res == -1)
		return -errno;
	return res;
}

static int wosfs_removexattr(const char *path, const char *name)
{
	int res = lremovexattr(path, name);
	if (res == -1)
		return -errno;
	return 0;
}
#endif /* HAVE_SETXATTR */

static struct fuse_operations wosfs_oper = {
	wosfs_getattr,
	wosfs_readlink,
       	NULL, 	// getdir
	wosfs_mknod,
	wosfs_mkdir,
	wosfs_unlink,
	wosfs_rmdir,
	wosfs_symlink,
	wosfs_rename,
	wosfs_link,
	wosfs_chmod,
	wosfs_chown,
	wosfs_truncate,
	NULL,	// fgetattr
	wosfs_open,
	wosfs_read,
	wosfs_write,
	wosfs_statfs,
 	NULL,	// flush
	wosfs_release,
	wosfs_fsync,
	NULL,	// setxattr
	NULL,	// getxattr
	NULL,	// listxattr
	NULL,	// removexattr
	NULL,  	// opendir
	wosfs_readdir,
	NULL, 	// releasedir
	NULL, 	// fsyncdir
	NULL, 	// init
   	NULL, 	// destroy
	wosfs_access,
	NULL, 	// create
	NULL,	// ftruncate
	NULL, 	// fgetattr
	NULL, 	// lock
	wosfs_utimens, 	// utimens
	NULL, 	// bmap
	0,	// flag_nopath and flag_reserve
	NULL, 	// ioctl
	NULL, 	// poll
	NULL, 	// write_buf
	NULL, 	// read_buf
	NULL, 	// flock
	NULL,	// fallocate
};


static int wosfs_opt_proc(void *data, const char *arg, int key, struct fuse_args *outargs)
{
     switch (key) {
     case KEY_HELP:
             fprintf(stderr,
                     "usage: %s mountpoint [options]\n"
                     "\n"
                     "general options:\n"
                     "    -o opt,[opt...]  mount options\n"
                     "    -h   --help      print help\n"
                     "    -V   --version   print version\n"
                     "\n"
                     "wosfs options:\n"
                     "    -m MAGIC 	   \t   WosFS magic number(default: DDNWOS)\n"          
                     "    -l path 	   \t   WosFS path in local file system tree\n"
                     "    -b path 	   \t   WosFS backup path in local file system tree\n"
                     "    -w <ip address>  \t   WOS cluster IP address to use\n"
                     "    -p policy 	   \t   WOS policy to use\n"
                     , outargs->argv[0]);
             fuse_opt_add_arg(outargs, "-ho");
             fuse_main(outargs->argc, outargs->argv, &wosfs_oper, NULL);
             exit(1);

     case KEY_VERSION:
             fprintf(stderr, "WosFS version %s\n", WOSFS_VERSION);
             fuse_opt_add_arg(outargs, "--version");
             fuse_main(outargs->argc, outargs->argv, &wosfs_oper, NULL);
             exit(0);
     }
     return 1;
}

extern "C" {

int fuse_main_real(int argc, char *argv[], const struct fuse_operations *op,
                   size_t op_size, void *user_data);

}

struct fuse_args args;
int main(int argc, char *argv[])
{
	int opt = 0;

	openlog("fusewos", 0, LOG_USER);

     	args = FUSE_ARGS_INIT(argc, argv);

     	memset(&wosfs_conf, 0, sizeof(wosfs_conf));
	wosfs_conf.wosfs_magic = wosfs_default_magic;
	wosfs_conf.wosfs_path = wosfs_default_path;
	wosfs_conf.wos_ip= wos_default_ip;
	wosfs_conf.wos_policy= wos_default_policy;
	wosfs_conf.wosfs_debug = WOSFS_LOG_ERRORS;

     	fuse_opt_parse(&args, &wosfs_conf, wosfs_opts, wosfs_opt_proc);

	WOSFS_DEBUGLOG(WOSFS_LOG_FILEOP, ":WOS:: wosfs_path=%s, wos_ip=%s, wos_policy=%s, wosfs_bak_path=%s", wosfs_conf.wosfs_path, wosfs_conf.wos_ip, wosfs_conf.wos_policy, wosfs_conf.wosfs_bak_path);

	if (pthread_mutex_init(&lock, NULL) != 0)
    	{
        	printf("\n mutex init failed\n");
        	return 1;
    	}

	if (pthread_mutex_init(&lock2, NULL) != 0)
    	{
        	printf("\n mutex init failed\n");
        	return 1;
    	}

	if (pthread_mutex_init(&lock_release, NULL) != 0)
    	{
        	printf("\n mutex init failed\n");
        	return 1;
    	}

	if (pthread_mutex_init(&lock_read, NULL) != 0)
    	{
        	printf("\n mutex init failed\n");
        	return 1;
    	}

	if (pthread_mutex_init(&lock_write, NULL) != 0)
    	{
        	printf("\n mutex init failed\n");
        	return 1;
    	}

	if (pthread_mutex_init(&lock_getattr, NULL) != 0)
    	{
        	printf("\n mutex init failed\n");
        	return 1;
    	}

	if (wosfs_parse_ns_paths(wosfs_conf.wosfs_path) == false )
		return 2;

	umask(0);

	wos_b.Connect(wosfs_conf.wos_ip);

	return fuse_main(args.argc, args.argv, &wosfs_oper, NULL);
}
