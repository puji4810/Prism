#include "block.h"
#include "coding.h"
#include "comparator.h"
#include "iterator.h"
#include "status.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <sys/types.h>

namespace prism
{
	inline uint32_t Block::NumRestarts() const
	{
		assert(size_ >= sizeof(uint32_t)); // make sure there is enough space for the restarts
		return DecodeFixed32(data_ + size_ - sizeof(uint32_t));
	}

	Block::Block(const BlockContents& contents)
	    : data_(contents.data.data())
	    , size_(contents.data.size())
	    , owned_(contents.heap_allocated)
	{
		if (size_ < sizeof(uint32_t))
		{
			size_ = 0;
		}
		else
		{
			size_t max_restarts_allowed = (size_ - sizeof(uint32_t)) / sizeof(uint32_t);
			if (NumRestarts() > max_restarts_allowed)
			{
				// size too small for the NumRestarts
				size_ = 0;
			}
			else
			{
				restart_offset_ = size_ - (1 + NumRestarts()) * sizeof(uint32_t);
			}
		}
	}

	Block::~Block()
	{
		if (owned_)
		{
			delete[] data_;
		}
	}

	// TODO: SIMD Decode?
	static inline const char* DecodeEntry(const char* p, const char* limit, uint32_t* shared, uint32_t* non_shared, uint32_t* value_length)
	{
		if (limit - p < 3) // A valid entry must be at least 3 bytes long, 1byte shared, 1byte non_shared, 1byte value_length
		{
			return nullptr;
		}
		// Entry
		// ┌─────────────────────────────────────┐
		// │ shared_bytes   (varint32)           │
		// │ unshared_bytes (varint32)           │
		// │ value_length   (varint32)           │
		// │ key_delta      (unshared_bytes)     │
		// │ value          (value_length)       │
		// └─────────────────────────────────────┘

		// Fast path
		*shared = reinterpret_cast<const uint8_t*>(p)[0];
		*non_shared = reinterpret_cast<const uint8_t*>(p)[1];
		*value_length = reinterpret_cast<const uint8_t*>(p)[2];
		if ((*shared | *non_shared | *value_length) < 128)
		{
			// the three all encoded into one byte each
			p += 3;
		}
		else
		{
			if ((p = GetVarint32Ptr(p, limit, shared)) == nullptr)
				return nullptr;
			if ((p = GetVarint32Ptr(p, limit, non_shared)) == nullptr)
				return nullptr;
			if ((p = GetVarint32Ptr(p, limit, value_length)) == nullptr)
				return nullptr;
		}

		// here, p reach the `key_delta` part
		// make sure the left space is enough for the (key_delta + value) bytes
		if (static_cast<uint32_t>(limit - p) < (*non_shared + *value_length))
		{
			return nullptr;
		}
		return p;
	}

	struct Block::Iter: public Iterator
	{
	private:
		const Comparator* const comparator_;
		const char* const data_; // block contents
		uint32_t const restarts_;
		uint32_t const num_restarts_;

		uint32_t current_; // The offset in data_ of current entry, >= restarts if !Valid, always points to the start of current entry
		uint32_t restart_index_; // Index of restart array
		std::string key_; // the entire Key value
		Slice value_;
		Status status_;

		inline int Compare(const Slice& a, const Slice& b) const { return comparator_->Compare(a, b); }

		// Return the offset in data_ of next entry
		inline uint32_t NextEntryOffset() const { return (value_.data() + value_.size()) - data_; }

		uint32_t GetRestartPoint(uint32_t index) const
		{
			assert(index < num_restarts_);
			return DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
		}

		void SeekToRestartPoint(uint32_t index)
		{
			key_.clear();
			restart_index_ = index;

			// current_ will be fixed by ParseNextKey()
			// the function will update the current_

			// ParseNextKey() starts at the end of the previous entry, so set the value accordingly
			uint32_t offset = GetRestartPoint(index);
			value_ = Slice(data_ + offset, 0);
		}

		void CorruptionError()
		{
			current_ = restarts_;
			restart_index_ = num_restarts_; // mark as invalid
			key_.clear();
			value_.clear();
			status_ = Status::Corruption("bad entry in block");
		}

		bool ParseNextKey()
		{
			current_ = NextEntryOffset();
			const char* p = data_ + current_;
			const char* limit = data_ + restarts_;

			if (p >= limit)
			{
				// No more entries to return
				// Mark as invalid
				current_ = restarts_;
				restart_index_ = num_restarts_;
				return false;
			}

			uint32_t shared, non_shared, value_length;
			p = DecodeEntry(p, limit, &shared, &non_shared, &value_length);

			if (p == nullptr || key_.size() < shared)
			{
				CorruptionError();
				return false;
			}
			else
			{
				key_.resize(shared); // we have make sure that key.size >= shared
				key_.append(p, non_shared);
				value_ = Slice(p + non_shared, value_length);
				while (restart_index_ + 1 < num_restarts_ && GetRestartPoint(restart_index_ + 1) < current_)
				{
					++restart_index_;
				}
				return true;
			}
		}

	public:
		Iter(const Comparator* comparator, const char* data, uint32_t restarts, uint32_t num_restarts)
		    : comparator_(comparator)
		    , data_(data)
		    , restarts_(restarts)
		    , num_restarts_(num_restarts)
		    , current_(restarts_)
		    , restart_index_(num_restarts_)
		{
			assert(num_restarts > 0);
		}

		bool Valid() const override { return current_ < restarts_; } // valid in [0, restarts_)

		Status status() const override { return status_; }

		Slice key() const override
		{
			assert(Valid());
			return key_;
		}

		Slice value() const override
		{
			assert(Valid());
			return value_;
		}

		void Next() override
		{
			assert(Valid());
			ParseNextKey();
		}

		void Prev() override
		{
			assert(Valid());
			// Scan backwards to find the previous entry
			uint32_t original = current_;
			while (GetRestartPoint(restart_index_) >= original)
			{
				if (restart_index_ == 0)
				{
					current_ = restarts_;
					restart_index_ = num_restarts_;
					return;
				}
				--restart_index_;
			}

			// move to the start point before
			SeekToRestartPoint(restart_index_);
			do
			{
				// Loop until end of current entry hits the start of original entry
			} while (ParseNextKey() && NextEntryOffset() < original);
		}

		void Seek(const Slice& target) override
		{
			// Binary search
			uint32_t left = 0;
			uint32_t right = num_restarts_ - 1;
			int current_key_compare = 0;

			if (Valid())
			{
				current_key_compare = Compare(key_, target);
				if (current_key_compare < 0)
				{
					left = restart_index_;
				}
				else if (current_key_compare > 0)
				{
					right = restart_index_;
				}
				else
				{
					return;
				}
			}

			while (left < right)
			{
				uint32_t mid = (left + right + 1) / 2;
				uint32_t region_offset = GetRestartPoint(mid);
				uint32_t shared, non_shared, value_length;
				const char* key_ptr = DecodeEntry(data_ + region_offset, data_ + restarts_, &shared, &non_shared, &value_length);

				if (key_ptr == nullptr || shared != 0)
				{
					CorruptionError();
					return;
				}

				Slice mid_key(key_ptr, non_shared);

				if (Compare(mid_key, target) < 0) // the last one which lower than target
				{
					left = mid;
				}
				else
				{
					right = mid - 1;
				}
			}
			// After binary search, linearly scan within the restart block
			assert(current_key_compare == 0 || Valid());
			bool skip_seek = left == restart_index_ && current_key_compare < 0;
			if (!skip_seek)
			{
				SeekToRestartPoint(left);
			}
			// Linear search (within restart block) for first key >= target
			while (true)
			{
				if (!ParseNextKey())
				{
					return;
				}
				if (Compare(key_, target) >= 0)
				{
					return;
				}
			}
		}

		void SeekToFirst() override
		{
			SeekToRestartPoint(0);
			ParseNextKey();
		}

		void SeekToLast() override
		{
			SeekToRestartPoint(num_restarts_ - 1);
			while (ParseNextKey() && NextEntryOffset() < restarts_)
			{
				// Keep skipping
			}
		}
	};

	Iterator* Block::NewIterator(const Comparator* comparator)
	{
		if (size_ < sizeof(uint32_t))
		{
			return NewErrorIterator(Status::Corruption("bad block contents"));
		}
		const uint32_t num_restarts = NumRestarts();
		if (num_restarts == 0)
		{
			return NewEmptyIterator();
		}
		else
		{
			return new Iter(comparator, data_, restart_offset_, num_restarts);
		}
	}
}
