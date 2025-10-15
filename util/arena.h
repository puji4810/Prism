#ifndef PRISM_UTIL_ARENA_H
#define PRISM_UTIL_ARENA_H

#include <cstddef>
#include <vector>
#include <atomic>

namespace prism
{
	class Arena
	{
	public:
		Arena();
		Arena(const Arena&) = delete;
		Arena& operator=(const Arena&) = delete;
		Arena(Arena&&) = delete;
		Arena& operator=(Arena&&) = delete;
		~Arena();
		char* Allocate(size_t bytes);
		char* AllocateAligned(size_t bytes);
		size_t MemoryUsage() const;

	private:
		char* AllocateNewBlock(size_t block_bytes);
		char* AllocateFallback(size_t bytes);
		
		char* alloc_ptr_;
		size_t alloc_bytes_remaining_;
		std::vector<char*> blocks_;
		std::atomic<size_t> memory_usage_;
	};
}

#endif