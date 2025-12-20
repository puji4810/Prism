#ifndef PRISM_MERGER_H_
#define PRISM_MERGER_H_

#include "comparator.h"
#include "iterator.h"

namespace prism
{
	// Return a new iterator that merges the contents of "children[0,n-1]".
	// Ownership of the iterators in children[] is transferred to the result and
	// they will be deleted when the result is deleted.
	Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children, int n);
}

#endif // PRISM_MERGER_H_

