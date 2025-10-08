#ifndef WRITE_BATCH_INTERNAL_H_
#define WRITE_BATCH_INTERNAL_H_

#include "write_batch.h"
#include <cstdint>

namespace prism
{

	// WriteBatchInternal provides static methods for manipulating a
	// WriteBatch that we don't want in the public WriteBatch interface.
	class WriteBatchInternal
	{
	public:
		using SequenceNumber = uint64_t;

		// Return the number of entries in the batch.
		static int Count(const WriteBatch* batch);

		// Set the count for the number of entries in the batch.
		static void SetCount(WriteBatch* batch, int n);

		static Slice Contents(const WriteBatch* batch) { return Slice(batch->rep_); }

		static size_t ByteSize(const WriteBatch* batch) { return batch->rep_.size(); }

		static SequenceNumber Sequence(const WriteBatch* batch);

		static void SetSequence(WriteBatch* batch, SequenceNumber seq);

		static void SetContents(WriteBatch* batch, const Slice& contents);

		static void Append(WriteBatch* dst, const WriteBatch* src);
	};

} // namespace prism

#endif