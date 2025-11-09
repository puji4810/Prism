#include "crc32c/crc32c.h"

namespace prism
{
	static inline uint32_t Unmask(uint32_t masked)
	{
		uint32_t rot = masked - 0xa282ead8u;
		return (rot << 15) | (rot >> 17);
	}

	static inline uint32_t Mask(uint32_t crc) { return ((crc >> 15) | (crc << 17)) + 0xa282ead8u; }
}