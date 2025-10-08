#ifndef PRISM_DBFORMAT_H
#define PRISM_DBFORMAT_H

#include <cstdint>
#include <string>

namespace prism
{
	enum class ValueType : uint8_t
	{
		kTypeDeletion = 0x0,
		kTypeValue = 0x1
	};
}

#endif // PRISM_DBFORMAT_H