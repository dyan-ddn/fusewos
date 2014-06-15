#pragma once
#include <string>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <stdint.h>

namespace wosapi {
   class WosObj;
   typedef boost::shared_ptr<WosObj> WosObjPtr;
   typedef std::string WosOID;

   class WosObj : private boost::noncopyable {
   protected:
      WosObj();

   public:
      static WosObjPtr Create();
      virtual ~WosObj() = 0;

      virtual const WosOID GetOID() const;

      virtual void SetMeta(const std::string& key, const std::string value) = 0;
      virtual void SetData(const void* ptr, uint64_t len) = 0;
      virtual void GetData(/*out*/const void*& ptr, /*out*/uint64_t& len) = 0;
      virtual void GetMeta(const std::string& key, /*out*/std::string& value) = 0;

      typedef void (*MetaVisitor)(void* p, const std::string& key, const std::string& value);
      virtual void EachMeta(void* p, MetaVisitor v) = 0;
   };
}
