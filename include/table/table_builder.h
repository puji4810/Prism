#ifndef PRISM_TABLE_BUILDER_H_
#define PRISM_TABLE_BUILDER_H_

#include "options.h"
#include "env.h"
#include "block.h"
#include "block_builder.h"
#include <cstdint>

namespace prism
{
	class TableBuilder
	{
	public:
		TableBuilder(const Options& options, WritableFile* file);

		TableBuilder(const TableBuilder&) = delete;
		TableBuilder& operator=(const TableBuilder&) = delete;
		TableBuilder(TableBuilder&&) = delete;
		TableBuilder& operator=(TableBuilder&&) = delete;

		// REQUIRES: Finish() of Abandon() has been called
		~TableBuilder();

		// Change the options
		// only some of options can be changed after construction, as others option filelds
		// not allowed to change dynamically
		// if a field which is not allowed to change and its value in the structure
		// is different from its value passed to the method, the method should return an error
		Status ChangeOptions(const Options& options);

		// Add a key-value pair to the table
		// REQUIRES:
		void Add(const Slice& key, const Slice& value);

		// Flush any kv pairs into file
		void Flush();

		// Return non-ok iff some error be detected
		Status status() const;

		// Finsh building
		// REQUIRES: Finish(), Abandon() have not been called
		Status Finish();

		// Should be anandoned or not?
		// Call Abandon() before destroying the builder
		// REQUIRES: Finish(), Abandon() have not been called
		void Abandon();

		// Number of calls to Add() so far.
		uint64_t NumEntries() const;

		// Size of the file generated so far.  If invoked after a successful
		// Finish() call, returns the size of the final generated file.
		uint64_t FileSize() const;

	private:
		bool ok() const { return status().ok(); }
		void WriteBlock(BlockBuilder* block, BlockHandle* handle);
		void WriteRawBlock(const Slice& data, CompressionType, BlockHandle* handle);

		struct Rep;
		Rep* rep_;
	};
}

#endif