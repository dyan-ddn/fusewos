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

#if 0
BlockWOSClient::BlockWOSClient(std::string cloud)
{
   wos = WosCluster::Connect(cloud);
}
#else
BlockWOSClient::BlockWOSClient()
{
}
#endif

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
//BlockWOSClient wos_b(host);
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

#define WOSFS_VERSION "0.1"
#define WOS_MAGIC "DDNWOS"
#define WOS_DEFAULT_PATH "/wosfs/"
#define WOS_DEFAULT_IP "10.44.34.73"
#define WOS_DEFAULT_POLICY "test"

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
     char *wos_ip;
     char *wos_policy;
} wosfs_conf;;

enum {
     KEY_HELP,
     KEY_VERSION,
};

#define WOSFS_OPT(t, p, v) { t, offsetof(struct wosfs_config, p), v }

static struct fuse_opt wosfs_opts[] = {
     WOSFS_OPT("-m %s",             wosfs_magic, 0),
     WOSFS_OPT("-l %s",       	   wosfs_path, 0),
     WOSFS_OPT("-w %s",       	   wos_ip, 0),
     WOSFS_OPT("-p %s",       	   wos_policy, 0),

     FUSE_OPT_KEY("-V",             KEY_VERSION),
     FUSE_OPT_KEY("--version",      KEY_VERSION),
     FUSE_OPT_KEY("-h",             KEY_HELP),
     FUSE_OPT_KEY("--help",         KEY_HELP),
     FUSE_OPT_END
};


bool test_wos_magic(FILE *fp)
{
	char magic[7];;

	syslog(LOG_INFO, "WOS:: test_wos_magic: IN");

	rewind(fp);
	syslog(LOG_INFO, "WOS:: test_wos_magic: Check 100");
        fread((void *)magic, 1, 6, fp); magic[6]=0;
	syslog(LOG_INFO, "WOS:: test_wos_magic: Check 101");
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
		syslog(LOG_INFO, "WOS::wosobj_info_last: failed to open file %s", path);
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
		syslog(LOG_INFO, "WOS::wosobj_info_last: file %s is empty", path);

	fclose(fp);

	return res;
}

struct wosclient_pool_entry* wosclient_pool_create_list(const char *path, WosPtr_t WosPtr)
{
    struct wosclient_pool_entry *ptr = (struct wosclient_pool_entry*)malloc(sizeof(struct wosclient_pool_entry));

    wosclient_pool_empty_count++;
    if ( NULL == ptr ) {
	syslog(LOG_INFO, "WOS:: wosclient_pool_create_list: failed to allocate memory for ptr");
	return NULL;
    }
    // must initialize to 0, otherwise strict check will complain and generate segment faults.
    memset(ptr, 0, sizeof(wosclient_pool_entry));

    ptr->WosPtr = WosPtr;
    ptr->path = (char *)malloc(strlen(path)+1);
    if ( NULL == ptr->path ) {
        syslog(LOG_ERR, "WOS::wosclient_pool_create_list: failed to allocate memory for ptr->path");
        return NULL;
    }
    strcpy((char *)ptr->path, path);
    ptr->next = NULL;

    wosclient_pool_head = wosclient_pool_curr = ptr;
    syslog(LOG_INFO, "WOS::wosclient_pool_create_list: OUT: wosclient_pool_empty_count=%d", wosclient_pool_empty_count);
    wosclient_pool_count++;
    return ptr;
}

struct wosclient_pool_entry* wosclient_pool_add_to_list(const char *path, WosPtr_t WosPtr, bool add_to_end)
{

    //pthread_mutex_lock(&lock);
    syslog(LOG_INFO, "WOS:: wosclient_pool_add_to_list: path=%s", path);

    if(NULL == wosclient_pool_head)
    {
    	syslog(LOG_INFO, "WOS::wosclient_pool_add_to_list: wosclient_pool is empty");
	//pthread_mutex_unlock(&lock);
        return (wosclient_pool_create_list(path, WosPtr));
    }

    struct wosclient_pool_entry *ptr = (struct wosclient_pool_entry*)malloc(sizeof(struct wosclient_pool_entry));
    if ( NULL == ptr ) {
        syslog(LOG_INFO, "WOS:: wosclient_pool_add_to_list: failed to allocate memory for ptr");
	//pthread_mutex_unlock(&lock);
        return NULL;
    }  
    memset(ptr, 0, sizeof(wosclient_pool_entry));

    ptr->WosPtr = WosPtr;
    ptr->path = (char *)malloc(strlen(path)+1);
    if ( NULL == ptr->path ) {
	syslog(LOG_ERR, "WOS::wosclient_pool_add_to_list: failed to allocate memory for ptr->path");
	//pthread_mutex_unlock(&lock);
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

    //pthread_mutex_unlock(&lock);
    return ptr;
}

struct wosclient_pool_entry* wosclient_pool_search_in_list_by_path(const char *path, struct wosclient_pool_entry **prev)
{
    struct wosclient_pool_entry *ptr = wosclient_pool_head;
    struct wosclient_pool_entry *tmp = NULL;
    bool found = false;

    //pthread_mutex_lock(&lock2);

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
        //pthread_mutex_unlock(&lock2);
        return ptr;
    }
    else
    {
        //pthread_mutex_unlock(&lock2);
        return NULL;
    }
}

int wosclient_pool_delete_call_count=0;
int wosclient_pool_delete_from_list(const char *path)
{
    struct wosclient_pool_entry *prev = NULL;
    struct wosclient_pool_entry *del = NULL;

    //pthread_mutex_lock(&lock);

    syslog(LOG_INFO, "WOS:: wosclient_pool_delete_from_list: path=%s, calls=%d, cur number: %d", path, ++wosclient_pool_delete_call_count, wosclient_pool_count);
    del = wosclient_pool_search_in_list_by_path(path,&prev);
    if(del == NULL)
    {
	//pthread_mutex_unlock(&lock);
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

    //pthread_mutex_unlock(&lock);

    return 0;
}

void wosclient_pool_print_list(void)
{
    struct wosclient_pool_entry *ptr = wosclient_pool_head;

    syslog(LOG_INFO, " -------Printing list Start------- ");
    while(ptr != NULL)
    {
        syslog(LOG_INFO, "[%s]",ptr->path);
        ptr = ptr->next;
    }
    syslog(LOG_INFO, " -------Printing list End------- ");

    return;
}


#define P_TEST "test"
WosOID store_obj_b(BlockWOSClient *wos_b, const char *pdata, size_t len)
{
	WosClusterPtr wos = wos_b->wos;	
	//WosPolicy policy = wos->GetPolicy(P_TEST);
	WosPolicy policy = wos->GetPolicy(wosfs_conf.wos_policy);

	// create an object; associate data, metadata with it 
	WosObjPtr obj = WosObj::Create();

	obj->SetData(pdata, len); 

	WosStatus rstatus; // return status 
	WosOID roid;// return oid
	wos->Put(rstatus, roid, policy, obj);
	if (rstatus != ok) {
		syslog(LOG_INFO,  "WOS:: store_obj_b: Error during Put: %s", rstatus.ErrMsg().c_str());
		return false; 
	}

	return roid;
}

#define WOS_USE_STREAM_API  1
bool store_obj_stream_b(WosPutStreamPtr wosps, const char *pdata, size_t len, off_t offset)
{
	WosStatus rstatus;

	wosps->PutSpan(rstatus, pdata, offset, len);	
	if (rstatus != ok) {
		syslog(LOG_ERR, "WOS:: store_obj_stream_b: Error in PutSpan %d", offset); 
	}
}

static int xmp_getattr(const char *path, struct stat *stbuf)
{
	int res;

	res = lstat(path, stbuf);
	if (res == -1)
		return -errno;

	if (S_ISREG(stbuf->st_mode)) {
      	if (strncmp (path, wosfs_conf.wosfs_path, strlen(wosfs_conf.wosfs_path)) == 0) {
		struct wosobj_info wosobj_info;
		memset(&wosobj_info, 0, sizeof(struct wosobj_info));
		if ( wosobj_info_last(path, &wosobj_info) == true )
			stbuf->st_size = wosobj_info.obj_len;
	}
	}

	syslog(LOG_INFO, "WOS:: xmp_getattr: path=%s, size=%d, res=%d", path, stbuf->st_size, res);
	return res;
}

static int xmp_access(const char *path, int mask)
{
	int res;

	res = access(path, mask);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_readlink(const char *path, char *buf, size_t size)
{
	int res;

	res = readlink(path, buf, size - 1);
	if (res == -1)
		return -errno;

	buf[res] = '\0';
	return 0;
}

static int xmp_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		       off_t offset, struct fuse_file_info *fi)
{
	DIR *dp;
	struct dirent *de;

	(void) offset;
	(void) fi;

	dp = opendir(path);
	if (dp == NULL)
		return -errno;

	while ((de = readdir(dp)) != NULL) {
		struct stat st;
		memset(&st, 0, sizeof(st));
		st.st_ino = de->d_ino;
		st.st_mode = de->d_type << 12;
		if (filler(buf, de->d_name, &st, 0))
			break;
	}

	closedir(dp);
	return 0;
}

static int xmp_mknod(const char *path, mode_t mode, dev_t rdev)
{
	int res;

	/* On Linux this could just be 'mknod(path, mode, rdev)' but this
	   is more portable */
	if (S_ISREG(mode)) {
		res = open(path, O_CREAT | O_EXCL | O_WRONLY, mode);
		if (res >= 0)
			res = close(res);
	} else if (S_ISFIFO(mode))
		res = mkfifo(path, mode);
	else
		res = mknod(path, mode, rdev);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_mkdir(const char *path, mode_t mode)
{
	int res;

	res = mkdir(path, mode);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_unlink(const char *path)
{
	int res;
	struct stat stbuf;

	res = lstat(path, &stbuf);
	if (res == -1)
		return -errno;

	syslog(LOG_INFO, "WOS:: xmp_unlink: IN : path=%s", path);

      	if (strncmp (path, wosfs_conf.wosfs_path,strlen(wosfs_conf.wosfs_path)) == 0) {
	if (S_ISREG(stbuf.st_mode)) {

        	struct wosobj_info wosobj_info;
        	memset(&wosobj_info, 0, sizeof(struct wosobj_info));
        	if ( wosobj_info_last(path, &wosobj_info) == false) {
			syslog(LOG_INFO, "WOS:: xmp_unlink: failed to read from file: %s", path);
		}	
		else if ( stbuf.st_nlink == 1 ) {
			WosStatus status;
			WosOID oid(wosobj_info.oid);
			wos_b.wos->Delete(status, oid); 
			if (status != ok) {
				syslog(LOG_INFO, "WOS:: xmp_unlink: WOS delete status=%s", status.ErrMsg().c_str());
			}
			syslog(LOG_INFO, "WOS:: xmp_unlink:: path=%s, oid=%s", path, wosobj_info.oid);
		}
	}
	}
	res = unlink(path);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_rmdir(const char *path)
{
	int res;

	res = rmdir(path);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_symlink(const char *from, const char *to)
{
	int res;

	res = symlink(from, to);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_rename(const char *from, const char *to)
{
	int res;

	syslog(LOG_INFO, "WOS:: xmp_rename: IN : from=%s, to=%s", from, to);

	res = rename(from, to);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_link(const char *from, const char *to)
{
	int res;

	res = link(from, to);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_chmod(const char *path, mode_t mode)
{
	int res;

	res = chmod(path, mode);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_chown(const char *path, uid_t uid, gid_t gid)
{
	int res;

	res = lchown(path, uid, gid);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_truncate(const char *path, off_t size)
{
	int res;

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

//#ifdef HAVE_UTIMENSAT
static int xmp_utimens(const char *path, const struct timespec ts[2])
{
	int res;

	/* don't use utime/utimes since they follow symlinks */
	res = utimensat(0, path, ts, AT_SYMLINK_NOFOLLOW);
	if (res == -1)
		return -errno;

	return 0;
}
//#endif

static int xmp_open(const char *path, struct fuse_file_info *fi)
{
	int res;

	syslog(LOG_INFO, "WOS: xmp_open: IN : path=%s", path); 
	res = open(path, fi->flags);
	if (res == -1)
		return -errno;

	close(res);

	return 0;
}

static int xmp_read(const char *path, char *buf, size_t size, off_t offset,
		    struct fuse_file_info *fi)
{
	int res = 0;

	syslog(LOG_INFO, "WOS: xmp_read: IN : path=%s, offset = %u, size = %u", path, offset, size); 
#if 0
        int fd;
        int res;

        (void) fi;
        fd = open(path, O_RDONLY);
        if (fd == -1)
                return -errno;

        res = pread(fd, buf, size, offset);
        if (res == -1)
                res = -errno;

        close(fd);
        return res;
#else
	(void)fi;

        struct stat stbuf;

        res = lstat(path, &stbuf);
        if (res == -1)
                return -errno;

      	if (strncmp (path, wosfs_conf.wosfs_path, strlen(wosfs_conf.wosfs_path)) != 0) {
	        int fd;

        	(void) fi;
        	fd = open(path, O_RDONLY);
        	if (fd == -1)
               		return -errno;

        	res = pread(fd, buf, size, offset);
        	if (res == -1)
                	res = -errno;

        	close(fd);
		syslog(LOG_INFO, "WOS: xmp_read: OUT : path=%s, offset = %u, size = %u, res = %d", path, offset, size, res); 

        	return res;
        }

	if (S_ISREG(stbuf.st_mode)) {

	pthread_mutex_lock(&lock_read);
        struct wosclient_pool_entry *wosclient = NULL;

        syslog(LOG_INFO, "WOS:: xmp_read:: %s, Check Point 1000", path);
        wosclient = wosclient_pool_search_in_list_by_path(path, NULL);
        syslog(LOG_INFO, "WOS:: xmp_read:: %s, Check Point 1001", path);
        if ( NULL == wosclient )        {
                if ( offset != 0 ) {
                        syslog(LOG_INFO, "WOS:: xmp_read: OUT : not reading from BOF: path=%s, offset=%u, size=%u", path, offset, size);
			pthread_mutex_unlock(&lock_read);
                        return -errno;
                }

                WosPtr_t WosPtr;
                WosPtr.get_bytes=0;

                char oid_str[41];
                struct wosobj_info wosobj_info;
                memset(&wosobj_info, 0, sizeof(struct wosobj_info));
        	syslog(LOG_INFO, "WOS:: xmp_read:: %s, Check Point 1002", path);
                if ( wosobj_info_last(path, &wosobj_info) == false ) {
			syslog(LOG_INFO, "WOS:: xmp_read: OUT : can not read stub file: %s", path);
			pthread_mutex_unlock(&lock_read);
			//return -errno;
			return EAGAIN;
		}

        	syslog(LOG_INFO, "WOS:: xmp_read:: %s, Check Point 1003", path);
                WosPtr.get_bytes = wosobj_info.obj_len;
		WosPtr.put_bytes = 0;

                WosOID oid(wosobj_info.oid);

                try {  
                        WosPtr.gs = wos_b.wos->CreateGetStream(oid);
                }
                catch (WosE_ObjectNotFound& e) {
                        syslog(LOG_INFO, "WOS:: xmp_read: OUT : Invalid OID: %s", oid.c_str());
			pthread_mutex_unlock(&lock_read);
                        return -errno;
                }
        	syslog(LOG_INFO, "WOS:: xmp_read:: %s, Check Point 1004", path);
                wosclient = wosclient_pool_add_to_list(path,WosPtr,true);
        	syslog(LOG_INFO, "WOS:: xmp_read:: %s, Check Point 1005", path);
		wosclient->type = WOS_READ;
		wosclient->len = WosPtr.get_bytes;
		
        }
#if 0
	if ( wosclient->WosPtr.put_bytes > 0 ) { 
		// read() right after write(), before release call, e.g. "git clone ssh://dyan@10.44.45.112/home/dyan/repo.git" does it
		// we can not really support this mode.
		// another case is: /mnt/fuse/wosfs/iozone/iozone -a -i 0 -i1 -f /mnt/fuse/wosfs/test.bin -n 64K -g 64K -q 4k -y 4k -w
		// this will do read before release for prevous write finishes.
		//   - one quick solution is to add wait cycles in read if detects an active release going on, but we can not add wait cycles for the case of active write.
		//   - or mount with single thread mode, which will impact performance: ./fusewos /mnt/fuse -l /wosfs/ -w 10.44.34.73 -p default -m WOSWOS -s -f &

	        WosStatus rstatus;
       	 	WosOID roid;

        	wosclient->WosPtr.ps->Close(rstatus, roid);

        	if (rstatus != ok) {
                	syslog(LOG_INFO, "WOS:: xmp_release: Error in closing PutSteam %d");
			pthread_mutex_unlock(&lock_read);
                	return -errno;
        	}

        	const char *oid_str= roid.c_str();

        	FILE * fp;
        	time_t sec;
        	sec = time (NULL);

        	fp = fopen (path, "a+");

        	fprintf(fp, "%s %s %lu %ld %s %s\n", wosfs_conf.wosfs_magic, oid_str, wosclient->WosPtr.put_bytes, sec, wosfs_conf.wos_ip, wosfs_conf.wos_policy);
        	fclose(fp);

		try {  
                        wosclient->WosPtr.gs = wos_b.wos->CreateGetStream(roid);
                }
                catch (WosE_ObjectNotFound& e) {
                        syslog(LOG_INFO, "WOS:: xmp_open: Invalid OID: %s", roid.c_str());
			pthread_mutex_unlock(&lock_read);
                        return -errno;
                }

		wosclient->len = wosclient->WosPtr.get_bytes = wosclient->WosPtr.put_bytes;
	}
#endif
	syslog(LOG_INFO, "WOS:: xmp_read: path=%s, offset=%d, size=%d, WosPtr.get_bytes=%d, length=%d", path, offset, size, wosclient->WosPtr.get_bytes, wosclient->len);
	if ( wosclient->len > 0 ) {
        	WosStatus rstatus;
		WosObjPtr robj;


		if ( offset > wosclient->len-1 ) {
			syslog(LOG_INFO, "WOS:: xmp_read: OUT : Invalid offset: offset=%d, length=%d", offset, wosclient->len);
			pthread_mutex_unlock(&lock_read);
		 	return -errno;
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

	pthread_mutex_unlock(&lock_read);
	return res;

#endif
}

static int xmp_write(const char *path, const char *buf, size_t size,
		     off_t offset, struct fuse_file_info *fi)
{
	int res=0;

        struct stat stbuf;

	syslog(LOG_INFO, "WOS: xmp_write: IN : path=%s, offset = %u, size = %u", path, offset, size); 

        res = lstat(path, &stbuf);
        if (res == -1)
                return -errno;

      	if (strncmp (path, wosfs_conf.wosfs_path, strlen(wosfs_conf.wosfs_path)) != 0) {
                return res;
        }

	pthread_mutex_lock(&lock_write);
	if (S_ISREG(stbuf.st_mode)) {
        struct wosclient_pool_entry *wosclient = NULL; 

        wosclient = wosclient_pool_search_in_list_by_path(path, NULL);
        if ( NULL == wosclient )        {
		if ( offset != 0 ) {
			syslog(LOG_INFO, "WOS:: xmp_write: not writing from BOF: path=%s, offset=%u, size=%u", path, offset, size);
			pthread_mutex_unlock(&lock_write);
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
                        //syslog(LOG_ERR, "WOS:: xmp_open: Invalid Policy: %s", P_TEST);
                        syslog(LOG_ERR, "WOS:: xmp_open: Invalid Policy: %s", wosfs_conf.wos_policy);
			pthread_mutex_unlock(&lock_write);
                        return -errno;
                }
                wosclient = wosclient_pool_add_to_list(path,WosPtr,true);
		wosclient->type = WOS_WRITE;
        }

	WosStatus rstatus;
	wosclient->WosPtr.ps->PutSpan(rstatus, (char *)buf, offset, size);
	if (rstatus != ok) {
		syslog(LOG_INFO, "WOS: xmp_write: Error in PutSpan at offset = %u with size = %u", offset, size); 
		pthread_mutex_unlock(&lock_write);
		return -errno;
	}
	wosclient->WosPtr.put_bytes += size;
	syslog(LOG_INFO, "WOS: xmp_write: OUT: path=%s, offset = %u, size = %u, put_bytes= %u", path, offset, size, wosclient->WosPtr.put_bytes); 
	res=size;
	}

	pthread_mutex_unlock(&lock_write);
	return res;
}

static int xmp_statfs(const char *path, struct statvfs *stbuf)
{
	int res;

	res = statvfs(path, stbuf);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_release(const char *path, struct fuse_file_info *fi)
{

	(void) fi;

	syslog(LOG_INFO, "WOS:: xmp_release: IN : path=%s, cur number=%d", path, wosclient_pool_count);
        struct stat stbuf;
	int res;

        res = lstat(path, &stbuf);
        if (res == -1)
                return -errno;

	pthread_mutex_lock(&lock_release);

        struct wosclient_pool_entry *wosclient = NULL;

        wosclient = wosclient_pool_search_in_list_by_path(path, NULL);
        if ( NULL == wosclient )        {
		pthread_mutex_unlock(&lock_release);
                syslog(LOG_INFO, "WOS:: xmp_release: OUT : no active wosclient found, path=%s", path);
                return res;
        }
#if 1
	if ( wosclient->type == WOS_READ ) {
		wosclient_pool_delete_from_list(path);
		pthread_mutex_unlock(&lock_release);
                syslog(LOG_INFO, "WOS:: xmp_release: OUT : called with active WOS_READ stream?! path=%s", path);
		return res;
	}
#endif
	if ( (wosclient->type == WOS_WRITE) && (wosclient->WosPtr.put_bytes == 0)) {
		wosclient_pool_delete_from_list(path);
		pthread_mutex_unlock(&lock_release);
                syslog(LOG_INFO, "WOS:: xmp_release: OUT : called with active WOS_WRITE stream, but put_bytes=%d, path=%s", wosclient->WosPtr.put_bytes, path);
		return res;
	}

	WosStatus rstatus;
	WosOID roid; 
	uint64_t put_bytes = wosclient->WosPtr.put_bytes;
	WosPutStreamPtr ps = wosclient->WosPtr.ps;

	wosclient_pool_delete_from_list(path);
	ps->Close(rstatus, roid);

        if (rstatus != ok) {
                syslog(LOG_INFO, "WOS:: xmp_release: OUT : Error in closing PutSteam %d");
		pthread_mutex_unlock(&lock_release);
		return -errno;
        }

        const char *oid_str= roid.c_str();

        FILE * fp;
	time_t sec;
	sec = time (NULL);

	fp = fopen (path, "a+");
	
	fprintf(fp, "%s %s %lu %ld %s %s\n", wosfs_conf.wosfs_magic, oid_str, put_bytes, sec, wosfs_conf.wos_ip, wosfs_conf.wos_policy);
   	fclose(fp);

	
	pthread_mutex_unlock(&lock_release);
	syslog(LOG_INFO, "WOS:: xmp_release: OUT : path=%s, cur number=%d", path, wosclient_pool_count);
	return res;
}

static int xmp_fsync(const char *path, int isdatasync,
		     struct fuse_file_info *fi)
{
	/* Just a stub.	 This method is optional and can safely be left
	   unimplemented */

	(void) path;
	(void) isdatasync;
	(void) fi;
	return 0;
}

#ifdef HAVE_POSIX_FALLOCATE
static int xmp_fallocate(const char *path, int mode,
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
static int xmp_setxattr(const char *path, const char *name, const char *value,
			size_t size, int flags)
{
	int res = lsetxattr(path, name, value, size, flags);
	if (res == -1)
		return -errno;
	return 0;
}

static int xmp_getxattr(const char *path, const char *name, char *value,
			size_t size)
{
	int res = lgetxattr(path, name, value, size);
	if (res == -1)
		return -errno;
	return res;
}

static int xmp_listxattr(const char *path, char *list, size_t size)
{
	int res = llistxattr(path, list, size);
	if (res == -1)
		return -errno;
	return res;
}

static int xmp_removexattr(const char *path, const char *name)
{
	int res = lremovexattr(path, name);
	if (res == -1)
		return -errno;
	return 0;
}
#endif /* HAVE_SETXATTR */

static struct fuse_operations xmp_oper = {
	xmp_getattr,
	xmp_readlink,
       	NULL, 	// getdir
	xmp_mknod,
	xmp_mkdir,
	xmp_unlink,
	xmp_rmdir,
	xmp_symlink,
	xmp_rename,
	xmp_link,
	xmp_chmod,
	xmp_chown,
	xmp_truncate,
	NULL,	// fgetattr
	xmp_open,
	xmp_read,
	xmp_write,
	xmp_statfs,
 	NULL,	// flush
	xmp_release,
	xmp_fsync,
	NULL,	// setxattr
	NULL,	// getxattr
	NULL,	// listxattr
	NULL,	// removexattr
	NULL,  	// opendir
	xmp_readdir,
	NULL, 	// releasedir
	NULL, 	// fsyncdir
	NULL, 	// init
   	NULL, 	// destroy
	xmp_access,
	NULL, 	// create
	NULL,	// ftruncate
	NULL, 	// fgetattr
	NULL, 	// lock
	xmp_utimens, 	// utimens
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
                     "    -m MAGIC 	   WOS FS magic number(default: DDNWOS)\n"          
                     "    -l path 	   WosFS  path in local file system tree\n"
                     "    -w ip address	   WOS cluster IP address to use\n"
                     "    -p policy 	   WOS policy to use\n"
                     , outargs->argv[0]);
             fuse_opt_add_arg(outargs, "-ho");
             fuse_main(outargs->argc, outargs->argv, &xmp_oper, NULL);
             exit(1);

     case KEY_VERSION:
             fprintf(stderr, "WosFS version %s\n", WOSFS_VERSION);
             fuse_opt_add_arg(outargs, "--version");
             fuse_main(outargs->argc, outargs->argv, &xmp_oper, NULL);
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

	//memset(wos_fs_path, 0, sizeof(wos_fs_path));
	//strcpy(wos_fs_path, WOS_DEFAULT_PATH);
	//syslog(LOG_INFO, "WOS:: wos_fs_path=%s, wos_ip=%s, wos_policy=%s", wos_fs_path, wos_ip[0], wos_policy[0]);

     	args = FUSE_ARGS_INIT(argc, argv);

     	memset(&wosfs_conf, 0, sizeof(wosfs_conf));
	wosfs_conf.wosfs_magic = wosfs_default_magic;
	wosfs_conf.wosfs_path = wosfs_default_path;
	wosfs_conf.wos_ip= wos_default_ip;
	wosfs_conf.wos_policy= wos_default_policy;

     	fuse_opt_parse(&args, &wosfs_conf, wosfs_opts, wosfs_opt_proc);

	syslog(LOG_INFO, "WOS:: wos_fs_path=%s, wos_ip=%s, wos_policy=%s", wosfs_conf.wosfs_path, wosfs_conf.wos_ip, wosfs_conf.wos_policy);

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

	umask(0);

	wos_b.Connect(wosfs_conf.wos_ip);

	//return fuse_main(argc, argv, &xmp_oper, NULL);
	return fuse_main(args.argc, args.argv, &xmp_oper, NULL);
}
