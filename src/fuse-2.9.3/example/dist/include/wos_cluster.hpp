#pragma once
#include <string>
#include <boost/shared_ptr.hpp>

#include "wos_obj.hpp"
#include "wos_policy.hpp"
#include "wos_exception.hpp"

#ifndef RELEASE
/*
 * Code marked ENGR_ONLY will be stripped out prior to shipping
 * this demo code
 */
#define ENGR_ONLY
#endif

namespace wos {
   class WosClusterImpl;
   typedef boost::shared_ptr<WosClusterImpl> WosClusterImplPtr;

   class WosPutStreamImpl;
   class WosGetStreamImpl;
   typedef boost::shared_ptr<WosGetStreamImpl> WosGetStreamImplPtr;
   typedef boost::shared_ptr<WosPutStreamImpl> WosPutStreamImplPtr;
};

namespace wosapi {
   typedef std::string WosOID;

   class WosCluster;
   typedef boost::shared_ptr<WosCluster> WosClusterPtr;

   class WosStatus {
   protected:
      int   m_value;
   public:
      WosStatus(int =0);
      int Value() const;
      operator int() const { return Value(); }
      std::string ErrMsg() const;
   };

   enum WosStatusType { ok, error, 
      NoNodeForPolicy         = 200,
      NoNodeForObject         = 201,
      UnknownPolicyName       = 202,
      InternalError           = 203,
      ObjectFrozen            = 204,
      InvalidObjId            = 205,
      NoSpace                 = 206,
      ObjNotFound             = 207,
      ObjCorrupted            = 208,
      FsCorrupted             = 209,
      PolicyNotSupported      = 210,
      IOErr                   = 211,
      InvalidObjectSize       = 212,
      MissingObject           = 213,
      TemporarilyNotSupported = 214,
      OutOfMemory             = 215,
      ReservationNotFound     = 216,
      EmptyObject             = 217,
      InvalidMetadataKey      = 218,
      UnusedReservation       = 219,
      WireCorruption          = 220,
      CommandTimeout          = 221,
      InvalidGetSpanMode      = 222,
      PutStreamAbandoned      = 223,
      IncompleteSearchMetadata        = 224,
      InvalidSearchMetadataTextLength = 225,
      InvalidIntegerSearchMetadata    = 226,
      InvalidRealSearchMetadata       = 227,
      ObjectComplianceReject          = 228,
      InvalidComplianceDate           = 229,
      _max_err_code  // insert new errors above this
   };

   // V1.1:
   class WosPutStream;
   class WosGetStream;
   typedef boost::shared_ptr<WosPutStream> WosPutStreamPtr;
   typedef boost::shared_ptr<WosGetStream> WosGetStreamPtr;

   // V2.0:
   enum BufferMode
   {
      Buffered,
      Unbuffered
   };

   enum IntegrityCheck
   {
      IntegrityCheckEnabled,
      IntegrityCheckDisabled
   };

   class WosCluster {
   protected:
      wos::WosClusterImplPtr impl;
      WosCluster(wos::WosClusterImplPtr);

   public:
      static WosClusterPtr Connect(const std::string& clustername);
      ~WosCluster();

      WosPolicy GetPolicy(std::string policy);
      std::string GetPolicyName(unsigned int id);

      // Public API

      // Blocking API:
      /// Store data (once-only)
      void Put(/*OUT*/ WosStatus&, /*OUT*/ WosOID&, WosPolicy pol, WosObjPtr wobj);

      /// Get an object
      void Get(/*OUT*/ WosStatus&, const WosOID& oid, /*OUT*/ WosObjPtr& wobj);

      /// Pre-reserve (lease?) an OID...
      void Reserve(/*OUT*/ WosStatus&, /*OUT*/ WosOID&, WosPolicy pol);

      /// Attach data to a pre-reserved OID (once-only)
      void PutOID(/*OUT*/ WosStatus&, const WosOID& oid, WosObjPtr wobj);

      /// Delete an object
      void Delete(/*OUT*/ WosStatus&, const WosOID& oid);

      /// Query for the existence of an object
      void Exists(/*OUT*/ WosStatus&, const WosOID& oid);


      /// Continuation-style...
      typedef void* Context;
      typedef void (*Callback)(WosStatus, WosObjPtr, Context);

      void Reserve(WosPolicy pol, Callback cb, Context ctx);
      void Put(WosObjPtr wobj, WosPolicy pol, Callback cb, Context ctx);

      void PutOID(WosObjPtr wobj, const WosOID& oid, Callback cb, Context ctx);
      void Get(const WosOID& oid, Callback cb, Context context);
      void Delete(const WosOID& oid, Callback cb, Context context);
      void Exists(const WosOID& oid, Callback cb, Context context);
     
      /// Wait for existing async operations to complete
      void Wait();

      /// Discover objects which have definitely been lost from multiple failures
      ///   Interestingly, if this returns true, the object is lost; if this returns false,
      ///   it is still possible that the object doesn't exist; perhaps the oid is invalid, etc.
      bool IsMissing(const WosOID& oid);

      // V1.1:

      // Create a Put Stream for creating a single (large) object:
      WosPutStreamPtr CreatePutStream(WosPolicy policy);

      // Create a Put Stream for use with a pre-reserved OID for creating a single (large) object:
      WosPutStreamPtr CreatePutOIDStream(WosOID oid);

      // Create a Get Stream for accessing a single (large) object:
      WosGetStreamPtr CreateGetStream(WosOID oid, bool prefetch_metadata =true);
#ifdef ENGR_ONLY
      wos::WosClusterImplPtr GetImpl() { return impl; }
#endif
   };

   /// A WosPutStream may be used to incrementally create an object which is
   ///   larger than fits in memory
   class WosPutStream {
   public:
      // typed imported from WosCluster:
      typedef WosCluster::Callback Callback;
      typedef WosCluster::Context Context;

      // Metadata
      void SetMeta(const std::string& key, const std::string& value);

      // Callback API
      void PutSpan(const void* data, uint64_t off, uint64_t len,
         Context context, Callback cb);
      void Close(Context context, Callback cb);

      // Blocking API
      void PutSpan(WosStatus& status, const void* data, 
         uint64_t off, uint64_t len);
      void Close(WosStatus& status, WosOID& oid);

      static const uint64_t ms_max_obj_size = 5ULL*1024*1024*1024*1024; // 5TB

   protected:
      wos::WosPutStreamImplPtr impl;
      WosPutStream(wos::WosPutStreamImplPtr);
   };

   /// A Get stream may be used to incrementally access parts of a 
   ///    large object
   class WosGetStream {
   public:
      // typed imported from WosCluster:
      typedef WosCluster::Callback Callback;
      typedef WosCluster::Context Context;
      typedef WosObj::MetaVisitor MetaVisitor;

      // Metadata
      void GetMeta(const std::string& key, /*OUT*/ std::string& value);
      void EachMeta(Context ctx, MetaVisitor v);

      // Callback API
      void GetSpan(uint64_t off, uint64_t len, Context context, Callback cb,
         BufferMode mode = Buffered,
         IntegrityCheck integrity_check = IntegrityCheckEnabled);

      // Blocking API
      void GetSpan(WosStatus& status, WosObjPtr& obj,
         uint64_t off, uint64_t len,
         BufferMode mode = Buffered,
         IntegrityCheck integrity_check = IntegrityCheckEnabled);

      // Return length of the data portion of the object:
      uint64_t GetLength() const;

   protected:
      wos::WosGetStreamImplPtr impl;
      WosGetStream(wos::WosGetStreamImplPtr);
   };
}
