#include "table_cache.h"
#include "env.h"
#include "table/table.h"
#include "coding.h"
#include "filename.h"

#include <memory>

namespace prism
{

	class TableCacheEntry
	{
	public:
		TableCacheEntry(std::unique_ptr<RandomAccessFile> file, std::unique_ptr<Table> table)
		    : file_(std::move(file))
		    , table_(std::move(table))
		{
		}

		Table* table() const { return table_.get(); }

	private:
		std::unique_ptr<RandomAccessFile> file_;
		std::unique_ptr<Table> table_;
	};

	static void DeleteEntry(const Slice& key, void* value) { delete reinterpret_cast<TableCacheEntry*>(value); }

	static void UnrefEntry(void* arg1, void* arg2)
	{
		Cache* cache = reinterpret_cast<Cache*>(arg1);
		Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
		cache->Release(h);
	}

	TableCache::TableCache(const std::string& dbname, const Options& options, int entries)
	    : env_(options.env)
	    , dbname_(dbname)
	    , options_(options)
	    , cache_(NewLRUCache(entries))
	{
	}

	TableCache::~TableCache() { delete cache_; }

	Status TableCache::FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle** handle)
	{
		Status s;
		char buf[sizeof(file_number)];
		EncodeFixed64(buf, file_number);
		Slice key(buf, sizeof(buf));
		*handle = cache_->Lookup(key);
		if (*handle == nullptr)
		{
			std::string fname = TableFileName(dbname_, file_number);
			Table* table = nullptr;
			auto file = env_->NewRandomAccessFile(fname);
			if (!file)
			{
				std::string old_fname = SSTTableFileName(dbname_, file_number);
				file = env_->NewRandomAccessFile(old_fname);
				if (!file)
				{
					return file.error();
				}
			}
			auto table_result = Table::Open(options_, file.value().get(), file_size, &table);
			if (!table_result.ok())
			{
				return table_result;
			}
			assert(table != nullptr);
			auto entry = std::make_unique<TableCacheEntry>(std::move(file.value()), std::unique_ptr<Table>(table));
			*handle = cache_->Insert(key, entry.release(), 1, &DeleteEntry);
		}
		return Status::OK();
	}

	Iterator* TableCache::NewIterator(const ReadOptions& options, uint64_t file_number, uint64_t file_size, Table** tableptr)
	{
		if (tableptr != nullptr)
		{
			*tableptr = nullptr;
		}

		Cache::Handle* handle = nullptr;
		Status s = FindTable(file_number, file_size, &handle);
		if (!s.ok())
		{
			return NewErrorIterator(s);
		}

		Table* table = reinterpret_cast<TableCacheEntry*>(cache_->Value(handle))->table();
		Iterator* result = table->NewIterator(options);
		result->RegisterCleanup(&UnrefEntry, cache_, handle);
		if (tableptr != nullptr)
		{
			*tableptr = table;
		}
		return result;
	}

	Status TableCache::Get(const ReadOptions& options, uint64_t file_number, uint64_t file_size, const Slice& k, void* arg,
	    prism::Table::HandleResult handle_result)
	{
		Cache::Handle* handle = nullptr;
		Status s = FindTable(file_number, file_size, &handle);
		if (s.ok())
		{
			Table* t = reinterpret_cast<TableCacheEntry*>(cache_->Value(handle))->table();
			s = t->InternalGet(options, k, arg, handle_result);
			cache_->Release(handle);
		}
		return s;
	}

	void TableCache::Evict(uint64_t file_number)
	{
		char buf[sizeof(file_number)];
		EncodeFixed64(buf, file_number);
		cache_->Erase(Slice(buf, sizeof(buf)));
	}
}
