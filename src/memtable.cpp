#include "memtable.h"
#include "arena.h"
#include "coding.h"
#include "slice.h"
#include <algorithm>
#include <utility>
#include <cassert>

namespace prism
{
	static Slice GetLengthPrefixedSlice(const char* data)
	{
		uint32_t len;
		const char* p = data;
		p = GetVarint32Ptr(p, p + 5, &len); // +5: we assume "p" is not corrupted
		return Slice(p, len);
	}

	MemTable::MemTable(const InternalKeyComparator& comparator)
	    : comparator_(comparator)
	    , refs_(0)
	    , arena_()
	    , table_(comparator_, &arena_) // use Keycomparator for SkipList
	{
	}

	MemTable::~MemTable() { assert(refs_ == 0); }

	void MemTable::Ref() { ++refs_; }

	void MemTable::Unref()
	{
		--refs_;
		assert(refs_ >= 0);
		if (refs_ == 0)
		{
			delete this;
		}
	}

	size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

	int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr) const
	{
		Slice a = GetLengthPrefixedSlice(aptr);
		Slice b = GetLengthPrefixedSlice(bptr);
		return comparator.Compare(a, b);
	}

	void MemTable::Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value)
	{
		// Format of an entry is:
		//  internal_key_size : varint32 of (key.size() + 8)
		//  key bytes         : char[key.size()]
		//  tag               : uint64 ((sequence << 8) | type)
		//  value_size        : varint32 of value.size()
		//  value bytes       : char[value.size()]
		const auto [key_size, val_size] = std::pair{ key.size(), value.size() };
		const auto internal_key_size = key_size + 8;
		const auto encoded_len = VarintLength(internal_key_size) + internal_key_size + VarintLength(val_size) + val_size;

		auto* buf = arena_.Allocate(encoded_len);
		auto* p = EncodeVarint32(buf, internal_key_size);

		std::memcpy(p, key.data(), key_size);
		p += key_size;

		EncodeFixed64(p, (seq << 8) | type);
		p += 8;

		p = EncodeVarint32(p, val_size);
		std::memcpy(p, value.data(), val_size);

		table_.Insert(buf);
	}

	bool MemTable::Get(const LookupKey& key, std::string* value, Status* s)
	{

		Slice memkey = key.memtable_key(); // use memtable_key for searching
		Table::Iterator iter(&table_);
		iter.Seek(memkey.data());
		if (iter.Valid())
		{
			// entry format is:
			//    klength  varint32
			//    userkey  char[klength]
			//    tag      uint64
			//    vlength  varint32
			//    value    char[vlength]
			// Check that it belongs to same user key.  We do not check the
			// sequence number since the Seek() call above should have skipped
			// all entries with overly large sequence numbers.
			const char* entry = iter.key();
			uint32_t key_length;
			const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
			if (comparator_.comparator.user_comparator()->Compare(Slice(key_ptr, key_length - 8), key.user_key()) == 0)
			{
				// correct user key
				const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
				switch (static_cast<ValueType>(tag & 0xff))
				{
				case kTypeValue: {
					Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
					value->assign(v.data(), v.size());
					return true;
				}
				case kTypeDeletion: {
					*s = Status::NotFound(Slice());
					return true;
				}
				}
			}
		}
		return false;
	}
}