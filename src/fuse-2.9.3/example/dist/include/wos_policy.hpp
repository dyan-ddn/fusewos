#pragma once
#include <string>

/*
 */
namespace wosapi {
class WosPolicy {
protected:
   unsigned int m_id;

public:
   WosPolicy();
   unsigned int GetId() const { return m_id; }
};

}
