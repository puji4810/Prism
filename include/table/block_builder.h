#ifndef PRISM_TABLE_BLOCK_BUILDER_H
#define PRISM_TABLE_BLOCK_BUILDER_H

// Block
// ┌─────────────────────────────────────┐
// │ Entry 1                             │
// │ Entry 2                             │
// │ ...                                 │
// │ Entry N                             │
// ├─────────────────────────────────────┤
// │ Restart Point 1 (4 bytes)           │
// │ Restart Point 2 (4 bytes)           │
// │ ...                                 │
// │ Restart Point M (4 bytes)           │
// ├─────────────────────────────────────┤
// │ Num Restarts (4 bytes)              │
// └─────────────────────────────────────┘

// Entry
// ┌─────────────────────────────────────┐
// │ shared_bytes   (varint32)           │
// │ unshared_bytes (varint32)           │
// │ value_length   (varint32)           │
// │ key_delta      (unshared_bytes)     │
// │ value          (value_length)       │
// └─────────────────────────────────────┘

// TODO:
// parallel construction: multiple BlockBuilders work at once
// direct I/O: bypassing the page cache to write straight to disk
// space pre-allocation: reducing filesystem metadata updates
// pipelined writes: building and compressing in parallel

#include "options.h"
#include "slice.h"
#include <cstdint>
#include <vector>

namespace prism
{
	class BlockBuilder
	{
	public:
		explicit BlockBuilder(const Options* options);

		BlockBuilder(const BlockBuilder&) = delete;
		BlockBuilder& operator=(const BlockBuilder&) = delete;
		BlockBuilder(const BlockBuilder&&) = delete;
		BlockBuilder& operator==(const BlockBuilder&&) = delete;

		void Reset();

		void Add(const Slice& key, const Slice& value);

		Slice Finish();

		size_t CurrentSizeEstimate() const;
		bool empty() const { return buffer_.empty(); }

	private:
		const Options* options_;
		std::string buffer_;
		std::vector<uint32_t> restarts_;
		int counter_; // counter since last restart point
		bool finished_;
		std::string last_key_;
	};
}

#endif