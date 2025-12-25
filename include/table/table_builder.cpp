#include "table_builder.h"
#include "coding.h"
#include "env.h"
#include "filter_policy.h"
#include "options.h"
#include "status.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "crc32.h"
#include "crc32c/crc32c.h"
#include <cassert>
#include <cstdint>
#include <memory>

namespace prism
{
	struct TableBuilder::Rep
	{
		Rep(const Options& opt, WritableFile* f)
		    : options(opt)
		    , index_block_options(opt)
		    , file(f)
		    , offset(0)
		    , data_block(&options)
		    , index_block(&index_block_options)
		    , num_entries(0)
		    , closed(false)
		    , pending_index_entry(false)
		{
			index_block_options.block_restart_interval = 1; // the interval of index block is 1
			if (options.filter_policy != nullptr)
			{
				filter_block = std::make_unique<FilterBlockBuilder>(options.filter_policy);
				filter_block->StartBlock(0);
			}
		}

		Options options;
		Options index_block_options;
		WritableFile* file;
		uint64_t offset;
		Status status;
		BlockBuilder data_block;
		BlockBuilder index_block;
		std::string last_key;
		int64_t num_entries; // the number of entres added by the builder
		bool closed; // Either Finish() or Abandon() has been called.
		std::unique_ptr<FilterBlockBuilder> filter_block;

		// We write the index when we see the first key in next block
		// This allows us use a shorter index key
		// As the last key in current block is "the apple"
		// the first key in next block is "the cat"
		// we can use "the b" as seq to satisfy the comparator
		// tha seq >= "the apple" && seq < "the cat"
		// Only true when data_block.empty() == true
		bool pending_index_entry;
		BlockHandle pending_handle;
		std::string compressed_output;
	};

	TableBuilder::TableBuilder(const Options& options, WritableFile* file)
	    : rep_(new Rep(options, file))
	{
	}

	TableBuilder::~TableBuilder()
	{
		assert(rep_->closed);
		delete rep_;
	}

	Status TableBuilder::ChangeOptions(const Options& options)
	{
		// Note: if more fields added, must update this function
		if (options.comparator != rep_->options.comparator)
		{
			return Status::InvalidArgument("changing comparator while building table");
		}
		if (options.filter_policy != rep_->options.filter_policy)
		{
			return Status::InvalidArgument("changing filter policy while building table");
		}

		rep_->options = options;
		rep_->index_block_options = options;
		rep_->index_block_options.block_restart_interval = 1;
		return Status::OK();
	}

	void TableBuilder::Add(const Slice& key, const Slice& value)
	{
		Rep* r = rep_;
		assert(!r->closed);
		if (!ok())
			return;
		if (r->num_entries > 0)
		{
			assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
		}

		if (r->pending_index_entry)
		{
			assert(r->data_block.empty());
			r->options.comparator->FindShortestSeparator(&r->last_key, key);
			std::string handle_encoding;
			r->pending_handle.EncodeTo(handle_encoding);
			r->index_block.Add(r->last_key, Slice(handle_encoding));
			r->pending_index_entry = false;
		}

		r->last_key.assign(key.data(), key.size());
		r->num_entries++;
		r->data_block.Add(key, value);
		if (r->filter_block != nullptr)
		{
			r->filter_block->AddKey(key);
		}

		if (r->data_block.CurrentSizeEstimate() >= r->options.block_size)
		{
			Flush();
		}
	}

	void TableBuilder::WriteRawBlock(const Slice& block_contents, CompressionType type, BlockHandle* handle)
	{
		Rep* r = rep_;
		handle->set_offset(r->offset);
		handle->set_size(block_contents.size());
		r->status = r->file->Append(block_contents);
		if (r->status.ok())
		{
			char trailer[kBlockTrailerSize];
			trailer[0] = static_cast<char>(type);
			uint32_t crc = crc32c::Crc32c(reinterpret_cast<const uint8_t*>(block_contents.data()), block_contents.size());
			// Extend crc to cover block type byte
			crc = crc32c::Extend(crc, reinterpret_cast<const uint8_t*>(trailer), 1);
			EncodeFixed32(trailer + 1, Mask(crc));
			r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
			if (r->status.ok())
			{
				r->offset += block_contents.size() + kBlockTrailerSize;
			}
		}
	}

	void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle)
	{
		// File format contains a sequence of blocks where each block has:
		//    block_data: uint8[n]
		//    type: uint8
		//    crc: uint32
		assert(ok());
		Rep* r = rep_;
		Slice raw = block->Finish();

		Slice block_contents;
		CompressionType type = r->options.compression;

		switch (type)
		{
		case CompressionType::kNoCompression:
			block_contents = raw;
			break;

		case CompressionType::kSnappyCompression:
			// TODO: implement Snappy compression
			// For now, fall back to no compression.
			block_contents = raw;
			type = CompressionType::kNoCompression;
			break;

		case CompressionType::kZstdCompression:
			// TODO: implement Zstd compression
			// For now, fall back to no compression.
			block_contents = raw;
			type = CompressionType::kNoCompression;
			break;
		}

		WriteRawBlock(block_contents, type, handle);
		r->compressed_output.clear();
		block->Reset();
	}

	void TableBuilder::Flush()
	{
		Rep* r = rep_;
		assert(!r->closed);
		if (!ok())
			return;
		if (r->data_block.empty())
			return;
		assert(!r->pending_index_entry);
		// Flush current data block to file and remember its handle.
		WriteBlock(&r->data_block, &r->pending_handle);
		if (ok())
		{
			r->pending_index_entry = true;
			r->status = r->file->Flush();
		}
		if (ok() && r->filter_block != nullptr)
		{
			r->filter_block->StartBlock(r->offset);
		}
	}

	Status TableBuilder::Finish()
	{
		Rep* r = rep_;
		Flush();
		assert(!r->closed);
		r->closed = true;

		BlockHandle filter_block_handle;
		BlockHandle metaindex_block_handle, index_block_handle;

		// Write filter block
		if (ok() && r->filter_block != nullptr)
		{
			WriteRawBlock(r->filter_block->Finish(), CompressionType::kNoCompression, &filter_block_handle);
		}

		// Write metaindex block
		if (ok())
		{
			Options meta_index_block_options = r->options;
			meta_index_block_options.comparator = BytewiseComparator();
			meta_index_block_options.block_restart_interval = 1;
			BlockBuilder meta_index_block(&meta_index_block_options);
			if (r->filter_block != nullptr)
			{
				// Add mapping from "filter.Name" to location of filter data
				std::string key = "filter.";
				key.append(r->options.filter_policy->Name());
				std::string handle_encoding;
				filter_block_handle.EncodeTo(handle_encoding);
				meta_index_block.Add(key, handle_encoding);
			}

			// TODO(postrelease): Add stats and other meta blocks
			WriteBlock(&meta_index_block, &metaindex_block_handle);
		}

		// Write index block
		if (ok())
		{
			if (r->pending_index_entry)
			{
				r->options.comparator->FindShortSuccessor(&r->last_key);
				std::string handle_encoding;
				r->pending_handle.EncodeTo(handle_encoding);
				r->index_block.Add(r->last_key, Slice(handle_encoding));
				r->pending_index_entry = false;
			}
			WriteBlock(&r->index_block, &index_block_handle);
		}

		// Write footer
		if (ok())
		{
			Footer footer;
			footer.set_metaindex_handle(metaindex_block_handle);
			footer.set_index_handle(index_block_handle);
			std::string footer_encoding;
			footer.EncodeTo(footer_encoding);
			r->status = r->file->Append(footer_encoding);
			if (r->status.ok())
			{
				r->offset += footer_encoding.size();
			}
		}
		return r->status;
	}

	Status TableBuilder::status() const { return rep_->status; }

	void TableBuilder::Abandon()
	{
		Rep* r = rep_;
		assert(!r->closed);
		r->closed = true;
	}

	uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

	uint64_t TableBuilder::FileSize() const { return rep_->offset; }
}
