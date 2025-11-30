#ifndef PRISM_TABLE_CACHE_H_
#define PRISM_TABLE_CACHE_H_

#include "cache.h"
#include "table/table.h"
#include "dbformat.h"

namespace prism
{
	class Env;

	class TableCache
	{
	public:
		TableCache(const std::string& dbname, const Options& options, int entries);

		TableCache(const TableCache&) = delete;
		TableCache& operator=(const TableCache&) = delete;

		~TableCache();

		// Return an iterator for the specified file number (the corresponding
		// file length must be exactly "file_size" bytes).  If "tableptr" is
		// non-null, also sets "*tableptr" to point to the Table object
		// underlying the returned iterator, or to nullptr if no Table object
		// underlies the returned iterator.  The returned "*tableptr" object is owned
		// by the cache and should not be deleted, and is valid for as long as the
		// returned iterator is live.
		Iterator* NewIterator(const ReadOptions& options, uint64_t file_number, uint64_t file_size, Table** tableptr = nullptr);

		// If a seek to internal key "k" in specified file finds an entry,
		// call (*handle_result)(arg, found_key, found_value).
		Status Get(const ReadOptions& options, uint64_t file_number, uint64_t file_size, const Slice& k, void* arg,
		    prism::Table::HandleResult handle_result);

		// Evict any entry for the specified file number
		void Evict(uint64_t file_number);

	private:
		Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**);

		Env* const env_;
		const std::string dbname_;
		const Options& options_;
		Cache* cache_;
	};
}

#endif