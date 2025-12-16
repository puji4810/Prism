#include "memtable.h"
#include "arena.h"
#include "coding.h"
#include "slice.h"
#include <utility>
#include <cassert>

namespace prism
{
	static Slice GetLengthPrefixedSlice(const char* data)
	{
		uint32_t len;
		const char* p = data;
		p = GetVarint32Ptr(p, p + kMaxVarint32Bytes, &len);
		return Slice(p, len);
	}

	static const char* EncodeKey(std::string& scratch, const Slice& target)
	{
		scratch.clear();
		PutVarint32(scratch, target.size());
		scratch.append(target.data(), target.size());
		return scratch.data();
	}

	MemTable::MemTable(const InternalKeyComparator& comparator)
	    : comparator_(comparator)
	    , refs_(0)
	    , arena_()
	    , table_(comparator_, &arena_) // use Keycomparator for SkipList
	{
	}

	MemTable::~MemTable() { assert(refs_ == 0); }

	class MemTableIterator: public Iterator
	{
	public:
		explicit MemTableIterator(const MemTable::Table* table)
		    : iter_(table)
		{
		}

		MemTableIterator(const MemTableIterator&) = delete;
		MemTableIterator& operator=(const MemTableIterator&) = delete;

		~MemTableIterator() override = default;

		bool Valid() const override { return iter_.Valid(); }
		void Seek(const Slice& k) override { iter_.Seek(EncodeKey(tmp_, k)); }
		void SeekToFirst() override { iter_.SeekToFirst(); }
		void SeekToLast() override { iter_.SeekToLast(); }
		void Next() override { iter_.Next(); }
		void Prev() override { iter_.Prev(); }
		Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
		Slice value() const override
		{
			Slice key_slice = GetLengthPrefixedSlice(iter_.key());
			return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
		}

		Status status() const override { return Status::OK(); }

	private:
		MemTable::Table::Iterator iter_;
		std::string tmp_; // For passing to EncodeKey
	};

	Iterator* MemTable::NewIterator() const { return new MemTableIterator(&table_); }

	std::unique_ptr<Iterator> MemTable::NewUniqueIterator() const { return std::make_unique<MemTableIterator>(&table_); }

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

	size_t MemTable::ApproximateMemoryUsage() const { return arena_.MemoryUsage(); }

	int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr) const noexcept
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

		if (key_size != 0)
			std::memcpy(p, key.data(), key_size);
		p += key_size;

		EncodeFixed64(p, (seq << 8) | type);
		p += 8;

		p = EncodeVarint32(p, val_size);
		if (val_size != 0)
			std::memcpy(p, value.data(), val_size);
		assert(p + val_size == buf + encoded_len);

		table_.Insert(buf);
	}

	[[nodiscard]] bool MemTable::Get(const LookupKey& key, std::string* value, Status* s)
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
			const char* key_ptr = GetVarint32Ptr(entry, entry + kMaxVarint32Bytes, &key_length);
			if (!key_ptr)
			{
				*s = Status::Corruption("Corrupted key");
				return false;
			}
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
