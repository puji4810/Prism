#include "block_builder.h"
#include "coding.h"
#include "comparator.h"
#include "options.h"
#include <cassert>
#include <cstddef>

namespace prism
{
	BlockBuilder::BlockBuilder(const Options* options)
	    : options_(options)
	    , restarts_()
	    , counter_(0)
	    , finished_(false)
	{
		assert(options->block_restart_interval >= 1);
		restarts_.push_back(0);
	}

	void BlockBuilder::Reset()
	{
		counter_ = 0;
		finished_ = false;
		restarts_.clear();
		restarts_.push_back(0);
		buffer_.clear();
		last_key_.clear();
	}

	size_t BlockBuilder::CurrentSizeEstimate() const
	{
		// Entry bytes + restart bytes + restart.num bytes
		return buffer_.size() + restarts_.size() * sizeof(uint32_t) + sizeof(uint32_t);
	}

	void BlockBuilder::Add(const Slice& key, const Slice& value)
	{
		Slice last_key_piece(last_key_);
		assert(!finished_);
		assert(counter_ <= options_->block_restart_interval);
		assert(buffer_.empty() || options_->comparator->Compare(key, last_key_piece) > 0);

		size_t shared = 0;
		if (counter_ < options_->block_restart_interval)
		{
			// find the largest previous shared string
			const size_t min_size = std::min(key.size(), last_key_piece.size());
			while (shared < min_size && last_key_[shared] == key[shared])
			{
				shared++;
			}
		}
		else
		{
			restarts_.push_back(buffer_.size());
			counter_ = 0;
		}
		const size_t non_shared = key.size() - shared; // non-shared bytes

		// Add "<shared><non_shared><value_size>" to buffer_
		PutVarint32(buffer_, shared);
		PutVarint32(buffer_, non_shared);
		PutVarint32(buffer_, value.size());

		// Add string delta to buffer_ followed by value
		buffer_.append(key.data() + shared, non_shared);
		buffer_.append(value.data(), value.size());

		// Update state
		last_key_.resize(shared);
		last_key_.append(key.data() + shared, non_shared);
		assert(Slice(last_key_) == key);
		counter_++;
	}

	Slice BlockBuilder::Finish()
	{
		// Append restart array
		for (size_t i = 0; i < restarts_.size(); i++)
		{
			PutFixed32(buffer_, restarts_[i]);
		}
		PutFixed32(buffer_, restarts_.size());
		finished_ = true;
		return Slice(buffer_);
	}
}