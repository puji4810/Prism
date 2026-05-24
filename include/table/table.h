#ifndef PRISM_TABLE_H_
#define PRISM_TABLE_H_

#include "env.h"
#include "iterator.h"
#include "options.h"
#include "block.h"
#include "status.h"
#include "format.h"
#include "filter_block.h"
#include "two_level_iterator.h"
#include "status.h"
#include "iterator.h"
#include <cstdint>

namespace prism
{
	class Block;
	class BlockHandle;
	class Footer;
	struct Options;
	class RandomAccessFile;
	struct ReadOptions;
	class TableCache;
	class FilterBlockReader;

	// A Table is a sorted map from strings to strings.  Tables are
	// immutable and persistent.  A Table may be safely accessed from
	// multiple threads without external synchronization.
	struct Table
	{
	public:
		// Attempt to open the table that is stored in bytes [0..file_size)
		// of "file", and read the metadata entries necessary to allow
		// retrieving data from the table.
		//
		// If successful, returns ok and sets "*table" to the newly opened
		// table.  The client should delete "*table" when no longer needed.
		// If there was an error while initializing the table, sets "*table"
		// to nullptr and returns a non-ok status.  Does not take ownership of
		// "*source", but the client must ensure that "source" remains live
		// for the duration of the returned table's lifetime.
		//
		// *file must remain live while this Table is in use.
		static Status Open(const Options& options, RandomAccessFile* file, uint64_t file_size, Table** table);

		Table(const Table&) = delete;
		Table& operator=(const Table&) = delete;
		Table(Table&&) = delete;
		Table& operator=(Table&&) = delete;

		~Table();

		// Returns a new iterator over the table contents.
		// The result of NewIterator() is initially invalid (caller must
		// call one of the Seek methods on the iterator before using it).
		Iterator* NewIterator(const ReadOptions& options) const;

		// Given a key, return an approximate byte offset in the file where
		// the data for that key begins (or would begin if the key were
		// present in the file).  The returned value is in terms of file
		// bytes, and so includes effects like compression of the underlying data.
		// E.g., the approximate offset of the last key in the table will
		// be close to the file length.
		uint64_t ApproximateOffsetOf(const Slice& key) const;

	private:
		friend class TableCache;
		// Test helper to exercise InternalGet in unit tests.
		friend Status TestInternalGetHelper(Table&, const ReadOptions&, const Slice&, std::string*, bool*);
		struct Rep;

		static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);
		static Iterator* BlockReaderByHandle(Table*, const ReadOptions&, const BlockHandle&);

		explicit Table(Rep* rep)
		    : rep_(rep)

		{
		}

		// Calls (*handle_result)(arg, ...) with the entry found after a call
		// to Seek(key).  May not make such a call if filter policy says
		// that key is not present.
		using HandleResult = Status (*)(void*, const Slice& k, const Slice& v);
		Status InternalGet(const ReadOptions& options, const Slice& key, void* arg, HandleResult handle_result);
		void ReadMeta(const Footer& footer);
		void ReadFilter(const Slice& filter_handle_value);

		Rep* const rep_;
	};

	struct Table::Rep
	{
		~Rep()
		{
			delete filter;
			delete[] filter_data;
			delete index_block;
		}

		Options options;
		Status status;
		RandomAccessFile* file;
		uint64_t cache_id = 0;

		FilterBlockReader* filter = nullptr;
		const char* filter_data = nullptr;

		BlockHandle metaindex_handle; // handle to metaindex_block
		Block* index_block = nullptr;
	};

} // namespace prism

namespace prism
{
}

#endif
