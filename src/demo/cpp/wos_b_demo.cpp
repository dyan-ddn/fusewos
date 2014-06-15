/**
 * Sample code to Put/Get objects to/from a WOS cluster
 *
 * This code demonstrates the blocking C++ API, with and without
 * threading
 *
 * Copyright 2008 DataDirect Networks, Inc.
 */
#include <cstdio>
#include <cstdlib>
#include <pthread.h>

#include <vector>

#include "wos_obj.hpp"
#include "wos_cluster.hpp"

/**
 * The maximum sized object which we can do a Put for.
 */
#define WSIZE_MAX (64*1024*1024-512)

using namespace wosapi;

/**
 * Class SharedRandomData
 * Provide a field of random data/strings from which we can
 * *quickly* construct random "objects"
 */
class SharedRandomData
{
   char* buf;
   int maxlen;
public:
   SharedRandomData(int maxsize);
   ~SharedRandomData();

   char* GetRandomBuffer(int len);
protected:
   int randint(int max);
};

/**
 * Set up a single random data buffer; we'll (randomly)
 * select continuous subsegments of this buffer later as
 * the source for BlockWosClient::PutRandom
 */
SharedRandomData::SharedRandomData(int maxsize)
{
   buf = new char[maxlen = maxsize];

   for (int i = 0; i < maxlen; ++i)
      buf[i] = randint(256);
}

/**
 * dtor
 */
SharedRandomData::~SharedRandomData()
{
   delete [] buf;
   buf = 0;
}

/**
 * Return a randomly selected "substring" from
 * our field of random data
 */
char*
SharedRandomData::GetRandomBuffer(int len)
{
   int off = random() % (maxlen - len);

   return buf + off;
}

/**
 * Returns an int in the range (0..max]
 * This is somehow better than random() % max
 */
int 
SharedRandomData::randint(int max) {
   int next = (int) (max * ((double) rand() / (RAND_MAX + 1.0)));
   return next;
}

/**
 ** A simple client that demonstrates blocking reads and writes.
 **/
class BlockWOSClient 
{
   SharedRandomData r;
   WosClusterPtr wos;
public:
   BlockWOSClient(std::string cloud, int maxsize);

   WosPolicy GetPolicy(std::string name);

   WosOID PutRandom(WosPolicy& pol, int max, unsigned* cksum =0);
   WosObjPtr Get(WosOID oid);

   WosOID Reserve(WosPolicy& pol);
   void PutOIDRandom(const WosOID& oid, int max, unsigned* cksum =0);

   unsigned Checksum(char* p, uint64_t len);
};

BlockWOSClient::BlockWOSClient(std::string cloud, int maxsize)
   : r(maxsize)
{
   wos = WosCluster::Connect(cloud);
}

WosPolicy
BlockWOSClient::GetPolicy(std::string name)
{
   return wos->GetPolicy(name);
}

/**
 * Adler32 (cribbed from wikipedia)
 */
unsigned
BlockWOSClient::Checksum(char* p, uint64_t len)
{
   unsigned a = 1, b = 0;

   while (len > 0) {
      int tlen = len > 5552 ? 5552 : len;
      len -= tlen;
      do {
         a += (*p++ & 0xff);
         b += a;
      } while (--tlen);

      a %= 65521;
      b %= 65521;
   }

   return (b << 16) | a;
}

WosOID
BlockWOSClient::PutRandom(WosPolicy& pol, int max, unsigned* cksum)
{
   int len = random() % max;
   char* data = r.GetRandomBuffer(len);
   if (cksum)
      *cksum = Checksum(data, len);

   WosObjPtr w = WosObj::Create();

   w->SetMeta("color", "bizarre");
   w->SetData(data, len);

   WosStatus s;
   WosOID oid;
   wos->Put(s, oid, pol, w);

   if (s != wosapi::ok)
      printf("error in put %d\n", s.Value());

   return oid;
}

WosObjPtr
BlockWOSClient::Get(WosOID oid)
{
   WosStatus s;
   WosObjPtr o;
   wos->Get(s, oid, o);
   return o;
}

WosOID 
BlockWOSClient::Reserve(WosPolicy& pol)
{
   WosStatus s;
   WosOID oid;
   wos->Reserve(s, oid, pol);

   if (s != wosapi::ok)
      printf("error in reserve %d\n", s.Value());

   return oid;
}

void 
BlockWOSClient::PutOIDRandom(const WosOID& oid, int max, unsigned* cksum)
{
   int len = random() % max;
   char* data = r.GetRandomBuffer(len);
   if (cksum)
      *cksum = Checksum(data, len);

   WosObjPtr w = WosObj::Create();

   w->SetMeta("color", "bizarre");
   w->SetData(data, len);

   WosStatus s;
   wos->PutOID(s, oid, w);

   if (s != wosapi::ok)
      printf("error in put w/ oid %d\n", s.Value());
}

struct datapair {
   WosOID	soid;
   unsigned	csum;

   datapair() : soid(), csum() {}
   datapair(const WosOID& oid, unsigned c) : soid(oid), csum(c) {}
};

struct Args {
   BlockWOSClient* bwc;
   WosPolicy pol;
   int max;
   int nobj;
};

/**
 * Put/Get objects... by means of ReserveOID
 */
void*
work2(void* x)
{
   Args* args = (Args*) x;

   std::vector<datapair> soids;

   BlockWOSClient& b = *args->bwc;

   printf("Doing Reserve-oid of %d objects\n", args->nobj);
   for (int i = 0; i < args->nobj; ++i)
   {
      WosOID oid = b.Reserve(args->pol);
      datapair p(oid, 0);
      soids.push_back(p);
   }

   printf("Doing Puts w/oid of %d objects\n", args->nobj);
   std::vector<datapair>::iterator it;
   for (it = soids.begin(); it != soids.end(); ++it)
   {
      unsigned csum = 0;
      b.PutOIDRandom(it->soid, args->max, &csum);
      it->csum = csum;
   }

   printf("Doing gets of %d objects\n", args->nobj);
   for (int i = 0; i < args->nobj; ++i)
   {
      int index = random() % args->nobj;
      WosObjPtr obj = b.Get(soids[index].soid);
      
      const void* p;
      uint64_t len;
      if (obj) {
         obj->GetData(p, len);
         unsigned cksum = b.Checksum((char*) p, len);
         if (cksum != soids[index].csum) {
            printf("checksum mismatch: got %08x expecting %08x\n", cksum, soids[index].csum);
         }
      }
   }

   return 0;
}

/**
 * Put/Get objects... 
 */
void*
work(void* x)
{
   Args* args = (Args*) x;

   std::vector<datapair> soids;

   BlockWOSClient& b = *args->bwc;

   printf("Doing puts of %d objects\n", args->nobj);
   for (int i = 0; i < args->nobj; ++i)
   {
      unsigned csum = 0;
      WosOID oid = b.PutRandom(args->pol, args->max, &csum);
      datapair p(oid, csum);
      soids.push_back(p);
   }

   printf("Doing gets of %d objects\n", args->nobj);
   for (int i = 0; i < args->nobj; ++i)
   {
      int index = random() % args->nobj;
      WosObjPtr obj = b.Get(soids[index].soid);
      
      const void* p;
      uint64_t len;
      if (obj) {
         obj->GetData(p, len);
         unsigned cksum = b.Checksum((char*) p, len);
         if (cksum != soids[index].csum) {
            printf("checksum mismatch: got %08x expecting %08x\n", cksum, soids[index].csum);
         }
      }
   }

   return 0;
}

void usage(char* cmd) {
   printf("usage: %s [-h] [-c cloud-name] [-p write-policy] [-w write-object-size] [-t thcount] [-T] [nobj]\n", cmd);
   printf("  -h:          help: this message\n");
   printf("  -c:          cloud name\n");
   printf("  -p:          policy\n");
   printf("  -w:          wsize: max write size\n");
   printf("  -t:          thread-count (0 means run in \"main\" thread)\n");
   printf("  -T:          use two-step reserve/put-oid scheme\n");
   printf("  nobj:        the number of objects to create/write\n");
}

struct Options {
   int optind;

   std::string cloud;
   std::string policy;
   int wsize;
   int nthreads;
   bool twostep;
   
   Options() : optind(), cloud("localhost"), policy("default"), wsize(4000), nthreads(0), twostep() {}
};

Options do_options(int argc, char** argv) {
   int opt;
   Options opts;

   while ((opt = getopt(argc, argv, "hm:c:p:w:t:T")) > 0) {
      switch (opt) {
      case 'h':
         usage(argv[0]);
         exit(1);
         break;

      case 'm':
      case 'c':
         opts.cloud = std::string(optarg);
         break;

      case 'p':
         opts.policy = std::string(optarg);
         break;

      case 'w':
         opts.wsize = atoi(optarg);
         if (opts.wsize > WSIZE_MAX) {
            printf("Max allowed write-object-size %d; adjusted\n", WSIZE_MAX);
            opts.wsize = WSIZE_MAX;
         }
         break;

      case 't':
         {
            int thcount = atoi(optarg);
            if (thcount < 0)
               thcount = 0;
            opts.nthreads = thcount;
         }
         break;

      case 'T':
         opts.twostep = true;
         break;

      case '?':
         printf("unrecognized option: %c\n", optopt);
         break;

      case ':':
         printf("missing argument\n");
         break;
      }
   }

   // Provide a way to deal with left-overs.
   opts.optind = optind;

   return opts;
}

int
main(int argc, char** argv)
{
   Options opts = do_options(argc, argv);
   int nobj = 10000;

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

   Args args;
   args.max = opts.wsize;
   args.nobj = nobj;

   BlockWOSClient b(opts.cloud, opts.wsize);
   args.bwc = &b;
   args.pol = b.GetPolicy(opts.policy);
   
   void *(*work_fcn)(void*) = opts.twostep ? work2 : work;

   if (opts.nthreads == 0)
      work_fcn(&args);
   else {
      pthread_t* threads = new pthread_t[opts.nthreads];

      // launch threads
      for (int i = 0; i < opts.nthreads; ++i)
      {
         if (!(0 == pthread_create(&threads[i], 0, work_fcn, &args)))
            printf("error: thread create\n");
      }

      // wait for them to complete
      void* rc = 0;
      for (int i = 0; i < opts.nthreads; ++i)
         pthread_join(threads[i], &rc);
   }

   return 0;
}

