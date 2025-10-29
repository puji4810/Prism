#ifndef MEMTABLE_H
#define MEMTABLE_H

#include "skiplist.h"
#include "arena.h"
#include "dbformat.h"
#include "status.h"
#include "iterator.h"
#include <memory>

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

		MemTable(const MemTable&) = delete;
		MemTable& operator=(const MemTable&) = delete;

		MemTable(MemTable&&) noexcept = delete;
		MemTable& operator=(MemTable&&) noexcept = delete;

		void Ref();
		void Unref();

		void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value);
		[[nodiscard]] bool Get(const LookupKey& key, std::string* value, Status* s);
		size_t ApproximateMemoryUsage() const;

		Iterator* NewIterator() const;
		std::unique_ptr<Iterator> NewUniqueIterator() const;

	private:
		~MemTable();
		struct KeyComparator
		{
			const InternalKeyComparator comparator;
			explicit KeyComparator(const InternalKeyComparator& c)
			    : comparator(c)
			{
			}
			int operator()(const char* a, const char* b) const noexcept;
		};

		friend class MemTableIterator;

		using Table = SkipList<const char*, KeyComparator>;
		KeyComparator comparator_;
		int refs_;
		Arena arena_;
		Table table_;
	};

}

#endif