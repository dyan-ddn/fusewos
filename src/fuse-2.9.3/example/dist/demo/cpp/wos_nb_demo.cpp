/*
 * wos_nb_demo.cpp
 *
 * This simple "app" demonstrates putting and gettings WosObjects via 
 * the continuation-style calling convention.
 */
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <fstream>
#include <sstream>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/algorithm/string.hpp>

#include "wos_policy.hpp"
#include "wos_cluster.hpp"
#include "wos_exception.hpp"


const unsigned long long GB = 1 << 30;
const unsigned long long WSIZE_MAX = 500ULL*GB - 512;
unsigned long long WSIZE = WSIZE_MAX;

const int REPORT_PERIOD_MAX = 10000;
int REPORT_PERIOD = REPORT_PERIOD_MAX;

const int MAXREADS = 1000000;

const int ERR_BUF_SZ = 64;

using namespace wosapi;
using namespace wos;

typedef boost::shared_ptr<std::fstream> FstreamPtr;
typedef boost::shared_ptr<std::vector<std::string> > SoidListPtr;

static bool readonly = false;
static bool writeonly = false;
static bool readall = false;
static bool readfix = false;
static bool showdelete = false;
static bool randomize = false;
static std::string oidfile = std::string("oids.txt");

// Forward:
struct UserMessageContext;
void mkRandomData(int max, char* p);
void writeobj2(UserMessageContext* mc);
void readobj2(UserMessageContext* mc, std::string soid);

/**
 * A simple utility timer class
 */
class Timer {
   struct timeval a;

public:
   // mark "now"
   void stamp();

   /// return this-other in seconds.
   double delta_sec(const Timer& other);
};

void Timer::stamp() {
    gettimeofday(&a, 0);
}

double Timer::delta_sec(const Timer& other) {
    time_t sec = a.tv_sec - other.a.tv_sec;
    suseconds_t usec = a.tv_usec - other.a.tv_usec;
    if (usec < 0) {
        usec += 1000000;
        sec -= 1;
    }

    double d = sec + usec/1000000.0;

    return d;
}

/**
 * WOS callback message context
 * This keeps track of the state associated with an in-flight object.
 */
struct UserMessageContext {
   WosClusterPtr f;
   WosPolicy p;

   FstreamPtr file;     // during PUT: write oids here
   FstreamPtr good;     // during GET -RA; good oids here
   FstreamPtr bad;      // during GET -RA; bad oids here

   char* data;
   int len;
   WosOID soid;

   std::string* oids;
   int max;
   Timer* stop;

   UserMessageContext() :
      f(), file(), good(), bad(), data(), len(),
      soid(), oids(), max(), stop() {}
};

// some global status... ugh.
volatile int nget = 0;
volatile int nput = 0;
volatile int nrsrv = 0;
volatile int nputoid = 0;
volatile int ndelete = 0;
volatile int ngetfailed = 0;
volatile int nputfailed = 0;

Timer lastreport;

// ugh: global buf ptr
char* buf = 0;

static void 
put_callback2(WosStatus s, WosObjPtr wobj, WosCluster::Context ctx) {
   static int n;
   nput++;
   UserMessageContext* mc = reinterpret_cast<UserMessageContext*>(ctx);

   if (!mc) {
      printf("bad msg context!\n");
      return;
   }

   mc->stop->stamp();

   switch (s) {
      case NoNodeForPolicy:
      case NoNodeForObject:
      case UnknownPolicyName:
      default:
         ++nputfailed;
         if (nputfailed < 10)
            printf("put failed %d: %s\n", (int)s, s.ErrMsg().c_str());
         delete mc;
         return;

      case ok:
      {
         // now do a get of this object...
         WosOID soid = wobj->GetOID();
         (*mc->file) << soid << std::endl;
      }
         break;
   }

   n++;

   if ((nput + REPORT_PERIOD) % REPORT_PERIOD == 0) {
      printf("PUT %8d/%8d objects ", nput, nputfailed);
      printf("FWPS: %6d ", (int)(n / -lastreport.delta_sec(*mc->stop)));
      lastreport = *mc->stop;
      n = 0;
      printf("\n");
   }

   delete mc;
}

static void 
get_callback2(WosStatus s, WosObjPtr wobj, WosCluster::Context ctx) {
   nget++;
   static int n;
   UserMessageContext* mc = reinterpret_cast<UserMessageContext*>(ctx);

   // stats:
   mc->stop->stamp();

   if (s != ok) {
      ++ngetfailed;
      if (ngetfailed < 10) {
         printf("get failed %d soid: '%s' %s\n", (int)s, 
            wobj ? wobj->GetOID().c_str() : mc->soid.c_str(), s.ErrMsg().c_str());
      }

      if (readall) {
         if (readfix)
            (*mc->bad) << mc->soid << std::endl;
      }
      else {
         delete mc;
         return;
      }
   }
   else
      if (readall && readfix)
         (*mc->good) << mc->soid << std::endl;

   n++;

   if ((nget + REPORT_PERIOD) % REPORT_PERIOD == 0) {
      printf("GET %8d/%8d objects ", nget, ngetfailed);
      printf("FRPS: %6d ", int(n / -lastreport.delta_sec(*mc->stop)));
      lastreport = *mc->stop;
      n = 0;
      printf("\n");
   }

   // Process get reply:
   const void* p;
   uint64_t nbytes;

   if (s == ok)
      wobj->GetData(/* out */ p, /* out */ nbytes);

   delete mc;
}

// Returns an int in the range [1..max]
int randint(int max) {
   int next = 1 + (int) (max * ((double) rand() / (RAND_MAX + 1.0)));
   return next;
}

void writeobj2(UserMessageContext* mc)
{
   UserMessageContext* mc2 = new UserMessageContext(*mc);

   WosObjPtr wobj = WosObj::Create();
   wobj->SetMeta("foo", "bar");
   unsigned long long wsize = randomize ? random() % WSIZE : WSIZE;

   // Elsewhere, the global "buf" points to a (single, shared) buf of
   // random data of size WSIZE.  We pick a subset of that here.
   int off = random() % WSIZE;
   //printf("wsize: %d, off: %d\n", wsize, off);
   mc2->data = buf+off;
   mc2->len = wsize;
   wobj->SetData(buf, wsize);

      mc->f->Put(wobj, mc->p, put_callback2, mc2);
}

void writeobjs(WosClusterPtr f, std::string p, int nobj) {
   WosPolicy policy = f->GetPolicy(p);

   FstreamPtr file(new std::fstream(oidfile.c_str(), std::fstream::out|std::fstream::app));

   Timer start, stop;
   start.stamp();

   UserMessageContext* mc;
   mc = new UserMessageContext();
   mc->f = f;
   mc->p = policy;

   mc->file = file;
   mc->stop = &stop;

   lastreport.stamp();
   for (int i = 0; i < nobj; i++) {
      writeobj2(mc);
   }

   // Wait for all transactions to complete...
   f->Wait();
   file->close();

   printf("FWPS: %6.2f; %d objs; %d fails\n", (nput-nputfailed)/stop.delta_sec(start), nput, nputfailed);

   delete mc;
}

void readobj2(UserMessageContext* mc, std::string soid)
{
   UserMessageContext* mc2 = new UserMessageContext(*mc);

   mc2->soid = soid;
   mc2->f->Get(soid, get_callback2, mc2);
}

void readobjs2(WosClusterPtr cx, int nobj=0) {
   FstreamPtr file(new std::fstream(oidfile.c_str(), std::fstream::in));

   Timer start, stop;
   start.stamp();

   printf("Starting gets\n");
   UserMessageContext* mc = new UserMessageContext();
   mc->f = cx;
   mc->file = file;
   mc->max = MAXREADS;
   mc->stop = &stop;

   /**
    * Read up to MAXREADS oids from the oids.txt file (likely stored
    *   from a series of previous runs
    * If the number of oids available is greater than MAXREADS, choose
    *   a fair distribution from all of the oids.
    * Finally, shuffle the oids so as to read them in a non-uniform
    *   order (note that this requires proper initialization of the rng).
    */
   mc->oids = new std::string[MAXREADS];
   for (int i =0; i < mc->max; ++i)
      mc->oids[i] = std::string();

   // Figure out how many oids are available, in total.
   int lines = 0;
   std::string line;
   while (getline(*(mc->file), line))
      lines++;

   if (lines == 0) {
      printf("No oids available to get\n");
      delete [] mc->oids;
      delete mc;
      return;
   }

   if (lines < MAXREADS) {
      lines = mc->max = lines;
   }

   printf("Read %d oids\n", lines);
   mc->file->clear();
   mc->file->seekg(0);

   int n = 0;
   int n2 = 0;
   while (getline(*(mc->file), line)){
      std::string soid;
      if (line == "")
         continue;

      std::vector<std::string> strs;
      boost::algorithm::split(strs, line, boost::is_any_of(" "));
      soid = strs[0];
      //printf(" +++ DEBUG %s \n", soid.c_str());

      int index = n;
      int x = n;
      if (n >= mc->max) {
         // for overflow, we replace a random index
         index = randint(mc->max)-1;
         x = randint(lines);		// probability of replacement
      }
      if (x < mc->max) {
         ++n2;
         mc->oids[index] = soid;
         //printf(" **** DEBUG %s \n", strs[0].c_str());
      }
      n++;
   }

   // shuffle mc->oids
   printf("Shuffling %d\n", mc->max);
   if (n < mc->max)
      mc->max = n;
      
   for (int i = 0; i < mc->max; ++i) {
      int index = randint(mc->max - i)-1;
      std::string tmp = mc->oids[index];
      mc->oids[index] = mc->oids[i];
      mc->oids[i] = tmp;
   }

   lastreport.stamp();
   nobj = nobj==0 ? mc->max : nobj;
   for (int i = 0; i < nobj; ++i) {
      readobj2(mc, mc->oids[i % mc->max]);
   }

   // Wait for all transactions to complete...
   cx->Wait();

   printf("FRPS: %6.2f %d objs; %d fails\n", (nget-ngetfailed)/stop.delta_sec(start), nget, ngetfailed);

   delete [] mc->oids;
   delete mc;
}

// Read *all* objects
void readobjs3(WosClusterPtr cx) {
   FstreamPtr file, goodfile, badfile;
   std::stringstream goodname, badname;

   file = FstreamPtr(new std::fstream(oidfile.c_str(), std::fstream::in));
   if (readfix) {
      goodname << oidfile << ".good";
      badname << oidfile << ".bad";

      goodfile = FstreamPtr(new std::fstream(goodname.str().c_str(), std::fstream::out));
      badfile = FstreamPtr(new std::fstream(badname.str().c_str(), std::fstream::out|std::fstream::app));
   }

   Timer start, stop;
   start.stamp();

   printf("Starting gets\n");
   UserMessageContext* mc = new UserMessageContext();
   mc->f = cx;
   mc->file = file;
   mc->max = MAXREADS;
   mc->stop = &stop;
   mc->good = goodfile;
   mc->bad = badfile;

   lastreport.stamp();
   std::string line;
   int c = 0;
   while (getline(*(mc->file), line)){
      std::string soid;
      if (line == "")
         continue;

      std::vector<std::string> strs;
      boost::algorithm::split(strs, line, boost::is_any_of(" "));
      soid = strs[0];
      //printf(" +++ DEBUG %s \n", soid.c_str());
      readobj2(mc, soid);
      c++;
   }
   //printf("submitted %d reads\n", c);

   // Wait for all transactions to complete...
   cx->Wait();

   if (readfix) {
      char err_buf[ERR_BUF_SZ];
      std::stringstream origname;
      origname << oidfile << ".orig";

      if (rename(oidfile.c_str(), origname.str().c_str()) != 0)
      {
          strerror_r(errno, err_buf, ERR_BUF_SZ);
          printf("Failed to rename %s -> %s: %s\n", oidfile.c_str(), origname.str().c_str(), err_buf);
      }

      if (rename(goodname.str().c_str(), oidfile.c_str()) != 0)
      {
          strerror_r(errno, err_buf, ERR_BUF_SZ);
          printf("Failed to rename %s -> %s: %s\n", goodname.str().c_str(), oidfile.c_str(), err_buf);
      }
   }

   printf("FRPS: %6.2f %d objs; %d fails\n", (nget-ngetfailed)/stop.delta_sec(start), nget, ngetfailed);

   delete mc;
}

/**
 * Assuming p points to an array of max char, put some random
 * data in this array
 */
void
mkRandomData(int max, char* p)
{
   for (int i = 0; i < max; i++)
      p[i] = randint(255);
}


void
work(WosClusterPtr cx, std::string policy, int nobj) {
   if (!readonly) {
      // Make general-purpose buffer of random bytes...
      buf = new char[WSIZE];
      if (WSIZE >= 50*1024*1024)
         printf("Making random data...\n");
      mkRandomData(WSIZE, buf);
      if (WSIZE >= 50*1024*1024)
      printf("done...\n");

      writeobjs(cx, policy, nobj);
   }

   if (!writeonly) {
      if (readall)
         readobjs3(cx);
      else
         readobjs2(cx, nobj);
   }
}

void usage(char* cmd) {
   printf("usage: %s [-h][-R] host [nobj]\n", cmd);
   printf("  -h:          help: this message\n");
   printf("  -W:          write-only; do not read back any records\n");
   printf("  -R:          read-only; do not write any records\n");
   printf("  -A:          read all oids in input file, once\n");
   printf("  -F:          when using -A, replace input file with successful read oids, bad ones go to .bad file\n");
   printf("  -c:          cloud name\n");
   printf("  -p:          write policy\n");
   printf("  -w:          wsize: max write size\n");
   printf("  -r:          choose random write size (bounded by wsize)\n");
   printf("  -o:          specify alternate oids.txt file\n");
   printf("  nobj:        the number of objects to create/write\n");
}

struct Options {
   std::string cloud;
   bool showdelete;
   int optind;
   std::string policy;
   int wsize;
   bool linktrace;
   std::string oidfile;
   bool random;
   
   Options() : cloud("localhost"), showdelete(), 
      optind(), policy("default"), wsize(4000), linktrace(), oidfile("oids.txt"), random() {}
};

Options do_options(int argc, char** argv) {
   int opt;
   Options opts;

   std::string ostr = "hp:DWRm:c:w:To:rAF";

   while ((opt = getopt(argc, argv, ostr.c_str())) > 0) {
      switch (opt) {
      case 'p':
         opts.policy = std::string(optarg);
         break;

      case 'D':
         showdelete = true;
         break;

      case 'R':
         readonly = true;
         break;

      case 'W':
         writeonly = true;
         break;

      case 'h':
         usage(argv[0]);
         exit(1);
         break;

      case 'w':
         opts.wsize = atoi(optarg);
         break;

      case 'm':
      case 'c':
         opts.cloud = std::string(optarg);
         break;

      case 'o':
         opts.oidfile = std::string(optarg);
         oidfile = opts.oidfile;
         break;

      case 'T':
         opts.linktrace = true;
         break;

      case 'r':
         opts.random = true;
         break;

      case 'A':
         readall = true;
         break;

      case 'F':
         readfix = true;
         break;

      case '?':
         printf("unrecognized option: %c\n", optopt);
         break;

      case ':':
         printf("missing argument\n");
         break;
      }
   }

   // Error checks;
   if (readfix && !readall) {
      readfix = false;
      printf("-F requires -A; -F ignored\n");
   }

   // Provide a way to deal with left-overs.
   opts.optind = optind;

   return opts;
}

int main(int argc, char** argv) {
   setlinebuf(stdout);

   std::string host("localhost");
   int nobj = 100 * 1000;

   Options opts = do_options(argc, argv);
   WSIZE = opts.wsize;

   // Find a suitable reporting period
   int ri = 360000000 / WSIZE;
   int m = 1;
   while (m*ri < REPORT_PERIOD_MAX && m < REPORT_PERIOD_MAX)
      if (10*m*ri < REPORT_PERIOD_MAX)
         m *= 10;
      else if (5*m*ri < REPORT_PERIOD_MAX)
         m *= 5;
      else if (2*m*ri < REPORT_PERIOD_MAX)
         m *= 2;
      else
         break;
   REPORT_PERIOD /= m;
   //REPORT_PERIOD = 1;

   switch (argc - opts.optind) {
      case 0:
         break;

      case 1:
         nobj = atoi(argv[opts.optind]);
         break;

      default:
         usage(argv[0]);
         exit(1);
	 break;
   }

   randomize = opts.random;
  
   if (readonly and writeonly) {
      printf("Cannot specify both read-only and write-only\n");
      exit(1);
   }

   if (readonly)
      printf("Read-only mode\n");
   else if (writeonly) 
      printf("Write-only mode\n");
   else
      printf("Writing %d objs, then reading them back\n", nobj);
   printf("cluster: %s; policy: %s\n", opts.cloud.c_str(), opts.policy.c_str());

   // WosCluster: connect to a storage cluster
   WosClusterPtr cluster;
   try {
      cluster = WosCluster::Connect(opts.cloud);
      work(cluster, opts.policy, nobj);
   }
   catch (WosE_CannotConnect& e) {
      printf("cannot connect to cluster\n");
   }

   return 0;
}
