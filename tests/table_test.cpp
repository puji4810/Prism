#include "table/table_builder.h"
#include "table/table.h"
#include "options.h"
#include "env.h"
#include "comparator.h"
#include "iterator.h"
#include <gtest/gtest.h>
#include <filesystem>
#include <vector>

using namespace prism;

namespace {
std::string TestFilePath() {
	// Use a file in the current working directory so we
	// don't depend on the process cwd being the project root.
	return "table_test.sst";
}
}

namespace prism
{
// Friend of Table to exercise the InternalGet template in tests.
Status TestInternalGetHelper(Table& table, const ReadOptions& options, const Slice& key, std::string* value_out, bool* found_out)
{
	*found_out = false;
	auto handle = [&](const Slice&, const Slice& v) {
		*found_out = true;
		*value_out = v.ToString();
	};
	return table.InternalGet(options, key, handle);
}
}

TEST(TableTest, BuildAndIterate) {
    Env* env = Env::Default();
    const std::string fname = TestFilePath();
    // Clean up if exists
    std::filesystem::remove(fname);

    Options options;
    options.comparator = BytewiseComparator();

    WritableFile* wf = nullptr;
    ASSERT_TRUE(env->NewWritableFile(fname, &wf).ok());

    TableBuilder builder(options, wf);
    std::vector<std::pair<std::string, std::string>> kvs = {
        {"a", "1"},
        {"b", "2"},
        {"c", "3"},
    };
    for (auto& kv : kvs) {
        builder.Add(Slice(kv.first), Slice(kv.second));
    }
    ASSERT_TRUE(builder.Finish().ok());
    ASSERT_TRUE(wf->Close().ok());
    delete wf;

    uint64_t file_size = std::filesystem::file_size(fname);

    RandomAccessFile* raf = nullptr;
    ASSERT_TRUE(env->NewRandomAccessFile(fname, &raf).ok());

    Table* table = nullptr;
    ASSERT_TRUE(Table::Open(options, raf, file_size, &table).ok());
    ASSERT_NE(table, nullptr);

    ReadOptions ro;
    std::unique_ptr<Iterator> it(table->NewIterator(ro));
    it->SeekToFirst();
    size_t idx = 0;
    for (; it->Valid(); it->Next(), ++idx) {
        EXPECT_LT(idx, kvs.size());
        EXPECT_EQ(it->key().ToString(), kvs[idx].first);
        EXPECT_EQ(it->value().ToString(), kvs[idx].second);
    }
    EXPECT_EQ(idx, kvs.size());

    delete table;
    delete raf;
    std::filesystem::remove(fname);
}

TEST(TableTest, InternalGet) {
	Env* env = Env::Default();
	const std::string fname = TestFilePath();
	std::filesystem::remove(fname);

	Options options;
	options.comparator = BytewiseComparator();

	WritableFile* wf = nullptr;
	ASSERT_TRUE(env->NewWritableFile(fname, &wf).ok());
	{
		TableBuilder builder(options, wf);
		builder.Add(Slice("a"), Slice("1"));
		builder.Add(Slice("b"), Slice("2"));
		builder.Add(Slice("c"), Slice("3"));
		ASSERT_TRUE(builder.Finish().ok());
	}
	ASSERT_TRUE(wf->Close().ok());
	delete wf;

	uint64_t file_size = std::filesystem::file_size(fname);
	RandomAccessFile* raf = nullptr;
	ASSERT_TRUE(env->NewRandomAccessFile(fname, &raf).ok());

	Table* table = nullptr;
	ASSERT_TRUE(Table::Open(options, raf, file_size, &table).ok());
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
	delete raf;
	std::filesystem::remove(fname);
}

int main(int argc, char** argv)
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
