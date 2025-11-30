#ifndef PRISM_HASH_H_
#define PRISM_HASH_H_

#include <cstddef>
#include <cstdint>

namespace prism
{
	uint32_t Hash(const char* data, size_t n, uint32_t seed);
}

#endif