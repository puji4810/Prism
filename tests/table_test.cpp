#include "table/table_builder.h"
#include "table/table.h"
#include "table_cache.h"
#include "coding.h"
#include "filter_policy.h"
#include "options.h"
#include "env.h"
#include "comparator.h"
#include "iterator.h"
#include "filename.h"
#include <gtest/gtest.h>
#include <filesystem>
#include <vector>

using namespace prism;

namespace
{
	std::string TestFilePath()
	{
		// Use a file in the current working directory so we
		// don't depend on the process cwd being the project root.
		return "table_test.sst";
	}

	class CountingRandomAccessFile: public RandomAccessFile
	{
	public:
		CountingRandomAccessFile(RandomAccessFile* file, uint64_t* read_calls, uint64_t* read_bytes)
		    : file_(file)
		    , read_calls_(read_calls)
		    , read_bytes_(read_bytes)
		{
		}

		Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override
		{
			(*read_calls_)++;
			(*read_bytes_) += n;
			return file_->Read(offset, n, result, scratch);
		}

	private:
		RandomAccessFile* file_;
		uint64_t* read_calls_;
		uint64_t* read_bytes_;
	};

	class ExactFilterPolicy: public FilterPolicy
	{
	public:
		const char* Name() const override { return "ExactFilter"; }

		void CreateFilter(const Slice* keys, int n, std::string* dst) const override
		{
			for (int i = 0; i < n; ++i)
			{
				PutVarint32(*dst, keys[i].size());
				dst->append(keys[i].data(), keys[i].size());
			}
		}

		bool KeyMayMatch(const Slice& key, const Slice& filter) const override
		{
			Slice input = filter;
			while (!input.empty())
			{
				uint32_t len = 0;
				if (!GetVarint32(&input, &len) || input.size() < len)
				{
					return true; // treat parsing errors as potential matches
				}
				if (Slice(input.data(), len) == key)
				{
					return true;
				}
				input.remove_prefix(len);
			}
			return false;
		}
	};
}

namespace prism
{
	// Friend of Table to exercise the InternalGet template in tests.
	Status TestInternalGetHelper(Table& table, const ReadOptions& options, const Slice& key, std::string* value_out, bool* found_out)
	{
		struct State
		{
			std::string* value_out;
			bool* found_out;
		};

		State state{ value_out, found_out };
		*found_out = false;
		auto handle = [](void* arg, const Slice&, const Slice& v) -> Status {
			auto* state = static_cast<State*>(arg);
			*state->found_out = true;
			*state->value_out = v.ToString();
			return Status::OK();
		};
		return table.InternalGet(options, key, &state, handle);
	}
}

TEST(TableTest, BuildAndIterate)
{
	Env* env = Env::Default();
	const std::string fname = TestFilePath();
	// Clean up if exists
	std::filesystem::remove(fname);

	Options options;
	options.comparator = BytewiseComparator();

	auto result = env->NewWritableFile(fname);
	ASSERT_TRUE(result.has_value());
	auto wf = std::move(result.value());

	TableBuilder builder(options, wf.get());
	std::vector<std::pair<std::string, std::string>> kvs = {
		{ "a", "1" },
		{ "b", "2" },
		{ "c", "3" },
	};
	for (auto& kv : kvs)
	{
		builder.Add(Slice(kv.first), Slice(kv.second));
	}
	ASSERT_TRUE(builder.Finish().ok());
	ASSERT_TRUE(wf->Close().ok());

	uint64_t file_size = std::filesystem::file_size(fname);

	// RandomAccessFile* raf = nullptr;
	auto raf = env->NewRandomAccessFile(fname);
	ASSERT_TRUE(raf);

	Table* table = nullptr;
	ASSERT_TRUE(Table::Open(options, raf.value().get(), file_size, &table).ok());
	ASSERT_NE(table, nullptr);

	ReadOptions ro;
	std::unique_ptr<Iterator> it(table->NewIterator(ro));
	it->SeekToFirst();
	size_t idx = 0;
	for (; it->Valid(); it->Next(), ++idx)
	{
		EXPECT_LT(idx, kvs.size());
		EXPECT_EQ(it->key().ToString(), kvs[idx].first);
		EXPECT_EQ(it->value().ToString(), kvs[idx].second);
	}
	EXPECT_EQ(idx, kvs.size());

	delete table;
	std::filesystem::remove(fname);
}

TEST(TableTest, InternalGet)
{
	Env* env = Env::Default();
	const std::string fname = TestFilePath();
	std::filesystem::remove(fname);

	Options options;
	options.comparator = BytewiseComparator();

	auto result = env->NewWritableFile(fname);
	ASSERT_TRUE(result.has_value());
	auto wf = std::move(result.value());
	{
		TableBuilder builder(options, wf.get());
		builder.Add(Slice("a"), Slice("1"));
		builder.Add(Slice("b"), Slice("2"));
		builder.Add(Slice("c"), Slice("3"));
		ASSERT_TRUE(builder.Finish().ok());
	}
	ASSERT_TRUE(wf->Close().ok());

	uint64_t file_size = std::filesystem::file_size(fname);
	// RandomAccessFile* raf = nullptr;
	auto raf = env->NewRandomAccessFile(fname);
	ASSERT_TRUE(raf);

	Table* table = nullptr;
	ASSERT_TRUE(Table::Open(options, raf.value().get(), file_size, &table).ok());
	ASSERT_NE(table, nullptr);

	ReadOptions ro;
	std::string value;
	bool found = false;

	// Existing key
	Status s = TestInternalGetHelper(*table, ro, Slice("b"), &value, &found);
	ASSERT_TRUE(s.ok());
	EXPECT_TRUE(found);
	EXPECT_EQ(value, "2");

	// Missing key
	value.clear();
	found = false;
	s = TestInternalGetHelper(*table, ro, Slice("x"), &value, &found);
	ASSERT_TRUE(s.ok());
	EXPECT_FALSE(found);

	delete table;
	std::filesystem::remove(fname);
}

TEST(TableTest, FilterBlockSkipsDataReads)
{
	Env* env = Env::Default();
	const std::string fname = TestFilePath();
	std::filesystem::remove(fname);

	ExactFilterPolicy policy;
	Options options;
	options.comparator = BytewiseComparator();
	options.filter_policy = &policy;

	auto result = env->NewWritableFile(fname);
	ASSERT_TRUE(result.has_value());
	auto wf = std::move(result.value());
	{
		TableBuilder builder(options, wf.get());
		builder.Add(Slice("a"), Slice("1"));
		builder.Add(Slice("b"), Slice("2"));
		builder.Add(Slice("c"), Slice("3"));
		ASSERT_TRUE(builder.Finish().ok());
	}
	ASSERT_TRUE(wf->Close().ok());

	uint64_t file_size = std::filesystem::file_size(fname);
	// RandomAccessFile* raf = nullptr;
	auto raf = env->NewRandomAccessFile(fname);
	ASSERT_TRUE(raf);

	uint64_t read_calls = 0;
	uint64_t read_bytes = 0;
	CountingRandomAccessFile counting_file(raf.value().get(), &read_calls, &read_bytes);

	Table* table = nullptr;
	ASSERT_TRUE(Table::Open(options, &counting_file, file_size, &table).ok());
	ASSERT_NE(table, nullptr);

	ReadOptions ro;
	std::string value;
	bool found = false;

	const uint64_t baseline_missing = read_calls;
	Status s = TestInternalGetHelper(*table, ro, Slice("bb"), &value, &found);
	ASSERT_TRUE(s.ok());
	EXPECT_FALSE(found);
	EXPECT_EQ(read_calls, baseline_missing);

	const uint64_t baseline_hit = read_calls;
	s = TestInternalGetHelper(*table, ro, Slice("b"), &value, &found);
	ASSERT_TRUE(s.ok());
	EXPECT_TRUE(found);
	EXPECT_EQ(value, "2");
	EXPECT_GT(read_calls, baseline_hit);

	delete table;
	std::filesystem::remove(fname);
}

TEST(TableTest, BlockCacheFillAndBypass)
{
	Env* env = Env::Default();
	const std::string fname = TestFilePath();
	std::filesystem::remove(fname);

	Options options;
	options.comparator = BytewiseComparator();

	auto result = env->NewWritableFile(fname);
	ASSERT_TRUE(result.has_value());
	auto wf = std::move(result.value());
	{
		TableBuilder builder(options, wf.get());
		builder.Add(Slice("a"), Slice("1"));
		builder.Add(Slice("b"), Slice("2"));
		builder.Add(Slice("c"), Slice("3"));
		ASSERT_TRUE(builder.Finish().ok());
	}
	ASSERT_TRUE(wf->Close().ok());

	uint64_t file_size = std::filesystem::file_size(fname);
	// RandomAccessFile* raf = nullptr;
	auto raf = env->NewRandomAccessFile(fname);
	ASSERT_TRUE(raf);

	std::unique_ptr<Cache> block_cache(NewLRUCache(1024 * 1024));
	options.block_cache = block_cache.get();

	Table* table = nullptr;
	ASSERT_TRUE(Table::Open(options, raf.value().get(), file_size, &table).ok());
	ASSERT_NE(table, nullptr);

	// 1) fill_cache = false -> should not populate block_cache
	ReadOptions ro_nocache;
	ro_nocache.fill_cache = false;
	std::string value;
	bool found = false;
	Status s = TestInternalGetHelper(*table, ro_nocache, Slice("b"), &value, &found);
	ASSERT_TRUE(s.ok());
	EXPECT_TRUE(found);
	EXPECT_EQ(value, "2");
	EXPECT_EQ(block_cache->TotalCharge(), 0u);

	// 2) fill_cache = true -> should populate block_cache
	ReadOptions ro_cache;
	ro_cache.fill_cache = true;
	value.clear();
	found = false;
	s = TestInternalGetHelper(*table, ro_cache, Slice("b"), &value, &found);
	ASSERT_TRUE(s.ok());
	EXPECT_TRUE(found);
	EXPECT_EQ(value, "2");

	delete table;
	std::filesystem::remove(fname);
}

// Characterization test: locks the current table-cache ownership contract
// before any RAII refactoring. Verifies that iterators keep the table alive
// through cache pinning even after cache eviction.
class TableCacheOwnershipTest: public ::testing::Test
{
protected:
	void SetUp() override
	{
		env_ = Env::Default();
		// Create a temporary directory for the test database
		dbname_ = std::filesystem::temp_directory_path() / "prism_table_cache_test";
		std::filesystem::create_directories(dbname_);

		// Build a small SSTable file
		Options options;
		options.comparator = BytewiseComparator();

		const uint64_t file_number = 1;
		std::string fname = TableFileName(dbname_.string(), file_number);

		auto result = env_->NewWritableFile(fname);
		ASSERT_TRUE(result.has_value());
		auto wf = std::move(result.value());
		{
			TableBuilder builder(options, wf.get());
			builder.Add(Slice("key1"), Slice("value1"));
			builder.Add(Slice("key2"), Slice("value2"));
			builder.Add(Slice("key3"), Slice("value3"));
			ASSERT_TRUE(builder.Finish().ok());
		}
		ASSERT_TRUE(wf->Close().ok());

		file_size_ = std::filesystem::file_size(fname);
		file_number_ = file_number;
	}

	void TearDown() override
	{
		// Clean up test database
		if (std::filesystem::exists(dbname_))
		{
			std::filesystem::remove_all(dbname_);
		}
	}

	Env* env_;
	std::filesystem::path dbname_;
	uint64_t file_number_;
	uint64_t file_size_;
};

TEST_F(TableCacheOwnershipTest, IteratorKeepsTableAliveAfterEvict)
{
	// This test verifies the ownership contract:
	// 1. TableCache::NewIterator returns an iterator that holds a cache handle
	// 2. The iterator registers cleanup to release the handle when destroyed
	// 3. Even after Evict(), the underlying TableAndFile stays alive
	//    because the iterator holds a reference via the cache handle

	Options options;
	options.comparator = BytewiseComparator();

	// Create a TableCache with capacity for 1 entry
	TableCache cache(dbname_.string(), options, 1);

	// Get an iterator from the cache
	ReadOptions read_options;
	std::unique_ptr<Iterator> iter(cache.NewIterator(read_options, file_number_, file_size_));
	ASSERT_TRUE(iter != nullptr);
	ASSERT_TRUE(iter->status().ok());

	// Evict the cache entry - this should NOT invalidate the iterator
	// because the iterator holds a reference to the cache entry
	cache.Evict(file_number_);

	// Verify the iterator still works after eviction
	// The iterator keeps the TableAndFile alive through cache pinning
	iter->SeekToFirst();
	ASSERT_TRUE(iter->Valid());
	EXPECT_EQ(iter->key().ToString(), "key1");
	EXPECT_EQ(iter->value().ToString(), "value1");

	iter->Next();
	ASSERT_TRUE(iter->Valid());
	EXPECT_EQ(iter->key().ToString(), "key2");
	EXPECT_EQ(iter->value().ToString(), "value2");

	iter->Next();
	ASSERT_TRUE(iter->Valid());
	EXPECT_EQ(iter->key().ToString(), "key3");
	EXPECT_EQ(iter->value().ToString(), "value3");

	iter->Next();
	EXPECT_FALSE(iter->Valid());

	// Iterator destruction will release the cache handle
}
