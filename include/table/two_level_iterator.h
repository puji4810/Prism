#ifndef PRISM_TWO_LEVEL_ITERATOR_H_
#define PRISM_TWO_LEVEL_ITERATOR_H_

#include "iterator.h"
#include "slice.h"

namespace prism
{
	struct ReadOptions;

	// BlockFunction converts an index iterator's value (typically an
	// encoded BlockHandle) into an iterator over the referenced block.
	using BlockFunction = Iterator* (*)(void* arg, const ReadOptions& options, const Slice& index_value);

	// Return a new two-level iterator. A two-level iterator contains an
	// index iterator whose values point to a sequence of blocks where
	// each block is itself a sequence of key/value pairs. The returned
	// iterator yields the concatenation of all key/value pairs in all
	// referenced blocks.
	//
	// Ownership of "index_iter" is transferred to the two-level iterator
	// and it will be deleted when the two-level iterator is destroyed.
	Iterator* NewTwoLevelIterator(Iterator* index_iter, BlockFunction block_function, void* arg, const ReadOptions& options);
}

#endif
