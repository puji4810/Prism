#ifndef MEMTABLE_H
#define MEMTABLE_H

#include "skiplist.h"
#include "arena.h"
#include "db.h"

// See docs/memtable.md for more details.
// MemTable
// ├── Arena
// ├── SkipList
// ├── KeyComparator : compare the internal key
// └── Ref Counting : manage the lifetime of the memtable
// 
namespace prism
{
	class InternalKeyComparator;
	class MemTableIterator;

	class MemTable
	{
	public:
		explicit MemTable(const InternalKeyComparator& comparator);
	};

}

#endif