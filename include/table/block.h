#ifndef PRISM_TABLE_BLOCK_H_
#define PRISM_TABLE_BLOCK_H_

#include "iterator.h"
#include "comparator.h"
#include "format.h"
#include <cstddef>
#include <cstdint>

namespace prism
{
	struct BlockContents;
	struct Comparator;

	struct Block
	{
	public:
		explicit Block(const BlockContents& contents);

		Block(const Block&) = delete;
		Block& operator=(const Block&) = delete;
		Block(Block&&) = delete;
		Block& operator=(Block&&) = delete;

		~Block();

		size_t size() const { return size_; }

		Iterator* NewIterator(const Comparator* comparator);

	private:
		size_t size_;
		const char* data_;
		uint32_t restart_offset_;
		bool owned_;

		class Iter;

		uint32_t NumRestarts() const;
	};

}

#endif
