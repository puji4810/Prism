#include "table.h"
#include "cache.h"
#include "coding.h"
#include <cstdint>

namespace prism
{

	Status Table::Open(const Options& options, RandomAccessFile* file, uint64_t file_size, Table** table)
	{
		*table = nullptr;
		if (file_size < Footer::kEncodedLength)
		{
			return Status::Corruption("file is too short");
		}

		char footer_space[Footer::kEncodedLength];
		Slice footer_input;
		Status s = file->Read(file_size - Footer::kEncodedLength, Footer::kEncodedLength, &footer_input, footer_space);
		if (!s.ok())
		{
			return s;
		}

		Footer footer;
		s = footer.DecodeFrom(footer_input);
		if (!s.ok())
		{
			return s;
		}

		BlockContents index_block_contents;
		ReadOptions opt;
		if (options.paranoid_checks)
		{
			opt.verify_checksums = true;
		}
		s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

		if (s.ok())
		{
			// We've successfully read the footer and the index block: we're
			// ready to serve requests.
			Block* index_block = new Block(index_block_contents);
			Rep* rep = new Table::Rep;
			rep->options = options;
			rep->file = file;
			rep->metaindex_handle = footer.metaindex_handle();
			rep->index_block = index_block;
			rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
			// rep->filter_data = nullptr;
			// rep->filter = nullptr;
			*table = new Table(rep);
			(*table)->ReadMeta(footer);
		}

		return s;
	}

	Table::~Table() { delete rep_; }

	void Table::ReadMeta(const Footer& footer)
	{
		// TODO: filter block support
		(void)footer;
	}

	void Table::ReadFilter(const Slice& filter_handle_value)
	{
		// TODO: filter block support
		(void)filter_handle_value;
	}

	static void DeleteBlock(void* arg, void* /*ignored*/) { delete reinterpret_cast<Block*>(arg); }

	static void DeleteCachedBlock(const Slice& /*key*/, void* value)
	{
		Block* block = reinterpret_cast<Block*>(value);
		delete block;
	}

	static void ReleaseBlock(void* arg, void* h)
	{
		Cache* cache = reinterpret_cast<Cache*>(arg);
		Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
		cache->Release(handle);
	}

	Iterator* Table::BlockReader(void* arg, const ReadOptions& options, const Slice& index_value)
	{
		Table* table = reinterpret_cast<Table*>(arg);
		Cache* block_cache = table->rep_->options.block_cache;
		Block* block = nullptr;
		Cache::Handle* cache_handle = nullptr;

		BlockHandle handle;
		Slice input = index_value;
		Status s = handle.DecodeFrom(input);

		if (s.ok())
		{
			BlockContents contents;
			if (block_cache != nullptr)
			{
				char cache_key_buffer[16];
				EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
				EncodeFixed64(cache_key_buffer + 8, handle.offset());
				Slice key(cache_key_buffer, sizeof(cache_key_buffer));

				cache_handle = block_cache->Lookup(key);
				if (cache_handle != nullptr)
				{
					block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
				}
				else
				{
					s = ReadBlock(table->rep_->file, options, handle, &contents);
					if (s.ok())
					{
						block = new Block(contents);
						if (contents.cachable && options.fill_cache)
						{
							cache_handle = block_cache->Insert(key, block, block->size(), &DeleteCachedBlock);
						}
					}
				}
			}
			else
			{
				s = ReadBlock(table->rep_->file, options, handle, &contents);
				if (s.ok())
				{
					block = new Block(contents);
				}
			}
		}

		Iterator* iter;
		if (block != nullptr)
		{
			iter = block->NewIterator(table->rep_->options.comparator);
			if (cache_handle == nullptr)
			{
				iter->RegisterCleanup(&DeleteBlock, block, nullptr);
			}
			else
			{
				iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
			}
		}
		else
		{
			iter = NewErrorIterator(s);
		}

		return iter;
	}

	Iterator* Table::NewIterator(const ReadOptions& options) const
	{
		return NewTwoLevelIterator(
		    rep_->index_block->NewIterator(rep_->options.comparator), &Table::BlockReader, const_cast<Table*>(this), options);
	}

	uint64_t Table::ApproximateOffsetOf(const Slice& key) const
	{
		Iterator* index_iter = rep_->index_block->NewIterator(rep_->options.comparator);
		index_iter->Seek(key);
		uint64_t result;
		if (index_iter->Valid())
		{
			BlockHandle handle;
			Slice input = index_iter->value();
			Status s = handle.DecodeFrom(input);
			if (s.ok())
			{
				result = handle.offset();
			}
			else
			{
				result = rep_->metaindex_handle.offset();
			}
		}
		else
		{
			result = rep_->metaindex_handle.offset();
		}

		delete index_iter;
		return result;
	}
}
