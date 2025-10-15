#include "arena.h"
#include <cassert>

namespace prism
{
	static const int kBlockSize = 4096;
	
	Arena::Arena()
		: alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0)
	{
	}

	Arena::~Arena()
	{
		for (char* block : blocks_)
		{
			delete[] block;
		}
	}

	char* Arena::Allocate(size_t bytes)
	{
		assert(bytes > 0);
		if (bytes <= alloc_bytes_remaining_)
		{
			char* result = alloc_ptr_;
			alloc_ptr_ += bytes;
			alloc_bytes_remaining_ -= bytes;
			return result;
		}
		return AllocateFallback(bytes); // use Fallback only if the bytes is not enough in the current block
	}

	char* Arena::AllocateAligned(size_t bytes)
	{
		const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
		static_assert((align & (align - 1)) == 0, "Pointer size should be a power of 2");
		size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1); // The offset of the current pointer
		size_t slop = (current_mod == 0 ? 0 : align - current_mod);
		size_t needed = bytes + slop; // The total number of bytes needed
		char* result;
		if (needed <= alloc_bytes_remaining_)
		{
			result = alloc_ptr_ + slop;
			alloc_ptr_ += needed;
			alloc_bytes_remaining_ -= needed;
		}
		else
		{
			result = AllocateFallback(bytes);
		}
		assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
		return result;
	}

	char* Arena::AllocateNewBlock(size_t block_bytes)
	{
		char* result = new char[block_bytes];
		blocks_.push_back(result);
		// Add the [] to the size and the char* (which is the 'result' pushed into the vector) to the size
		memory_usage_.fetch_add(block_bytes + sizeof(char*), std::memory_order_relaxed);
		return result;
	}

	char* Arena::AllocateFallback(size_t bytes)
	{
		if (bytes > kBlockSize / 4)
		{
			char* result = AllocateNewBlock(bytes);
			return result;
		}

		// if Bytes is less than a quarter of the block size, we waste the remaining space in the current block
		// and allocate a new block
		alloc_ptr_ = AllocateNewBlock(kBlockSize);
		alloc_bytes_remaining_ = kBlockSize;

		char* result = alloc_ptr_;
		alloc_ptr_ += bytes;
		alloc_bytes_remaining_ -= bytes;
		return result;
	}

	size_t Arena::MemoryUsage() const
	{
		return memory_usage_.load(std::memory_order_relaxed);
	}
}