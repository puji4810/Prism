#include "write_batch.h"
#include "status.h"
#include "write_batch_internal.h"
#include "dbformat.h"
#include "coding.h"

namespace prism
{
	WriteBatch::WriteBatch() { rep_.resize(kHeader); }

	void WriteBatch::Clear()
	{
		rep_.clear();
		rep_.resize(kHeader);
	}

	size_t WriteBatch::ApproximateSize() const { return rep_.size(); }

	void WriteBatch::Put(const Slice& key, const Slice& value)
	{
		WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
		rep_.push_back(static_cast<char>(ValueType::kTypeValue));
		PutLengthPrefixedSlice(rep_, key);
		PutLengthPrefixedSlice(rep_, value);
	}

	void WriteBatch::Delete(const Slice& key)
	{
		WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
		rep_.push_back(static_cast<char>(ValueType::kTypeDeletion));
		PutLengthPrefixedSlice(rep_, key);
	}

	void WriteBatch::Append(const WriteBatch& source) { WriteBatchInternal::Append(this, &source); }

	Status WriteBatch::Iterate(Handler* handler) const
	{
		Slice input(rep_);
		if (input.size() < kHeader)
		{
			return Status::Corruption("malformed WriteBatch (too small)");
		}

		input.remove_prefix(kHeader);
		Slice key, value;
		int found = 0;
		while (!input.empty())
		{
			found++;
			char tag = input[0];
			input.remove_prefix(1);
			switch (tag)
			{
			case static_cast<char>(ValueType::kTypeValue):
				if (GetLengthPrefixedSlice(&input, &key) && GetLengthPrefixedSlice(&input, &value))
				{
					handler->Put(key, value);
				}
				else
				{
					return Status::Corruption("bad WriteBatch Put");
				}
				break;
			case static_cast<char>(ValueType::kTypeDeletion):
				if (GetLengthPrefixedSlice(&input, &key))
				{
					handler->Delete(key);
				}
				else
				{
					return Status::Corruption("bad WriteBatch Delete");
				}
				break;
			default:
				return Status::Corruption("unknown WriteBatch tag");
			}
		}
		if (found != WriteBatchInternal::Count(this))
		{
			return Status::Corruption("WriteBatch has wrong count");
		}
		else
		{
			return Status::OK();
		}
	}

	int WriteBatchInternal::Count(const WriteBatch* b) { return DecodeFixed32(b->rep_.data() + 8); }

	void WriteBatchInternal::SetCount(WriteBatch* b, int n) { EncodeFixed32(&b->rep_[8], n); }

	WriteBatchInternal::SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) { return DecodeFixed64(b->rep_.data()); }

	void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) { EncodeFixed64(&b->rep_[0], seq); }

	void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents)
	{
		assert(contents.size() >= kHeader);
		b->rep_.assign(contents.data(), contents.size());
	}

	void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src)
	{
		SetCount(dst, Count(dst) + Count(src));
		assert(src->rep_.size() >= kHeader);
		dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
	}

}