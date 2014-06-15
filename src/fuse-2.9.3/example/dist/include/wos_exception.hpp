#pragma once
#include <exception>
#include <boost/shared_ptr.hpp>

namespace wosapi {
   // Exceptions

   class WosException;
   typedef boost::shared_ptr<WosException> WosExceptionPtr;
   class WosException : public std::exception {
   public:
      const char* what() const throw() { return "(generic) WosException"; }
      virtual WosExceptionPtr clone() { return WosExceptionPtr(new WosException(*this)); }
      virtual void raise() { throw *this; }
   };

#define WOSE_CLONE(T) \
   WosExceptionPtr clone() { return WosExceptionPtr(new T(*this)); } \
   void raise() { throw *this; }

   class WosE_CannotConnect : public WosException {
   public:
      const char* what() const throw() { return "cannot connect to cluster"; }
      WOSE_CLONE(WosE_CannotConnect);
   };

   class WosE_AlreadyConnected : public WosException {
   public:
      const char* what() const throw() { return "already connected to cluster"; }
      WOSE_CLONE(WosE_AlreadyConnected);
   };

   class WosE_ConnectionLost : public WosException {
   public:
      const char* what() const throw() { return "connection to cluster lost"; }
      WOSE_CLONE(WosE_ConnectionLost);
   };

   class WosE_NotImpl : public WosException {
   public:
      const char* what() const throw() { return "not implemented"; }
      WOSE_CLONE(WosE_NotImpl);
   };

   class WosE_LeaseExpired : public WosException {
   public:
      const char* what() const throw() { return "WosE_LeaseExpired"; }
      WOSE_CLONE(WosE_LeaseExpired);
   };

   class WosE_NoSuitablePolicy : public WosException {
   public:
      const char* what() const throw() { return "WosE_NoSuitablePolicy"; }
      WOSE_CLONE(WosE_NoSuitablePolicy);
   };

   class WosE_WrongFieldType : public WosException {
   public:
      const char* what() const throw() { return "WosE_WrongFieldType"; }
      WOSE_CLONE(WosE_WrongFieldType);
   };

   class WosE_WriteIncomplete : public WosException {
   public:
      const char* what() const throw() { return "WosE_WriteIncomplete"; }
      WOSE_CLONE(WosE_WriteIncomplete);
   };

   class WosE_UnsupportedCommand : public WosException {
   public:
      const char* what() const throw() { return "WosE_UnsupportedCommand"; }
      WOSE_CLONE(WosE_UnsupportedCommand);
   };

   class WosE_ObjectFrozen : public WosException {
   public:
      const char* what() const throw() { return "Object can no longer be written"; }
      WOSE_CLONE(WosE_ObjectFrozen);
   };

   class WosE_InvalidObjectSize : public WosException {
   public:
      const char* what() const throw() { return "Object size/length is invalid"; }
      WOSE_CLONE(WosE_InvalidObjectSize);
   };

   class WosE_MissingObject : public WosException {
   public:
      const char* what() const throw() { return "WosObjPtr is null"; }
      WOSE_CLONE(WosE_MissingObject);
   };

   class WosE_MissingCallback : public WosException {
   public:
      const char* what() const throw() { return "Callback function pointer is null"; }
      WOSE_CLONE(WosE_MissingCallback);
   };

   class WosE_ObjectTooBig : public WosException {
   public:
      const char* what() const throw() { return "Objects must be < 5TB"; }
      WOSE_CLONE(WosE_ObjectTooBig);
   };

   class WosE_PutSpanNotContiguous : public WosException {
   public:
      const char* what() const throw() { return "Put spans must be contiguous"; }
      WOSE_CLONE(WosE_PutSpanNotContiguous);
   };

   class WosE_MaxMetaCount : public WosException {
   public:
      const char* what() const throw() { return "Objects support 32700 pieces of metadata"; }
      WOSE_CLONE(WosE_MaxMetaCount);
   };

   class WosE_InvalidKeyName : public WosException {
   public:
      const char* what() const throw() { return "Metadata keys must be between 1 and 64 bytes long and must not begin with ':'"; }
      WOSE_CLONE(WosE_InvalidKeyName);
   };

   class WosE_InvalidDateFormat: public WosException {
   public:
      const char* what() const throw() { return "Incorrect format specified for date. The format must be yyyy-mm-dd hh:mm[:ss]"; }
      WOSE_CLONE(WosE_InvalidDateFormat);
   };

   class WosE_InvalidObjectFormat : public WosException {
   public:
      const char* what() const throw() { return "Internal object format is invalid"; }
      WOSE_CLONE(WosE_InvalidObjectFormat);
   };

   class WosE_InvalidOffset : public WosException {
   public:
      const char* what() const throw() { return "Object offset is out-of-bounds"; }
      WOSE_CLONE(WosE_InvalidOffset);
   };

   class WosE_InvalidLength : public WosException {
   public:
      const char* what() const throw() { return "Object length is out-of-bounds"; }
      WOSE_CLONE(WosE_InvalidLength);
   };

   class WosE_CannotAllocate : public WosException {
   public:
      const char* what() const throw() { return "Out of memory during allocation"; }
      WOSE_CLONE(WosE_CannotAllocate);
   };

   class WosE_InvalidReservation : public WosException {
   public:
      const char* what() const throw() { return "Invalid reservation for use with a PutStream"; }
      WOSE_CLONE(WosE_InvalidReservation);
   };

   class WosE_UnusedReservation : public WosException {
   public:
      const char* what() const throw() { return "Unused reservation - cannot create a GetStream"; }
      WOSE_CLONE(WosE_UnusedReservation);
   };

   class WosE_ObjectNotFound : public WosException {
   public:
      const char* what() const throw() { return "Object not found"; }
      WOSE_CLONE(WosE_ObjectNotFound);
   };

   class WosE_ObjectCorrupted : public WosException {
   public:
      const char* what() const throw() { return "Object corrupted"; }
      WOSE_CLONE(WosE_ObjectCorrupted);
   };

   class WosE_StreamFrozen : public WosException {
   public:
      const char* what() const throw() { return "Stream cannot be written after close"; }
      WOSE_CLONE(WosE_StreamFrozen);
   };

   class WosE_StreamInvalid : public WosException {
   public:
      const char* what() const throw() { return "Stream has encountered an unrecoverable error"; }
      WOSE_CLONE(WosE_StreamInvalid);
   };

   class WosE_InvalidPolicy : public WosException {
   public:
      const char* what() const throw() { return "Policy is invalid"; }
      WOSE_CLONE(WosE_InvalidPolicy);
   };

   class WosE_TooManyThreads : public WosException {
   public:
      const char* what() const throw() { return "Cannot support >200 threads"; }
      WOSE_CLONE(WosE_TooManyThreads);
   };

   class WosE_EmptyStream : public WosException {
   public:
      const char* what() const throw() { return "Cannot call PutSpan() with length 0 or Close() without a prior PutSpan() or SetMeta()"; }
      WOSE_CLONE(WosE_EmptyStream);
   };

   class WosE_WaitOnWrongThread : public WosException {
   public:
      const char* what() const throw() { return "Wait must be invoked on the same thread as Connect"; }
      WOSE_CLONE(WosE_WaitOnWrongThread);
   };

   class WosE_InvalidBufferMode : public WosException {
   public:
      const char* what() const throw() { return "Buffer mode is invalid"; }
      WOSE_CLONE(WosE_InvalidBufferMode);
   };

   class WosE_InvalidIntegrityCheckValue : public WosException {
   public:
      const char* what() const throw() { return "Integrity check enable value is invalid"; }
      WOSE_CLONE(WosE_InvalidIntegrityCheckValue);
   };

   class WosE_InvalidGetSpanMode : public WosException {
   public:
      const char* what() const throw() { return "Disabling the object integrity check for buffered GetSpans is not allowed"; }
      WOSE_CLONE(WosE_InvalidGetSpanMode);
   };

#undef WOSE_CLONE
}
