#include "db.h"
#include "write_batch.h"

#include <gtest/gtest.h>
#include <filesystem>

using namespace prism;

// Test fixture for DB tests
class DBTest: public ::testing::Test
{
protected:
	void SetUp() override
	{
		// Clean up any existing test databases before each test
		std::error_code ec;
		std::filesystem::remove_all("test_db", ec);
		std::filesystem::remove_all("test_db_large", ec);
		std::filesystem::remove_all("test_db_multi_large", ec);
	}

	void TearDown() override
	{
		// Clean up after each test
		std::error_code ec;
		std::filesystem::remove_all("test_db", ec);
		std::filesystem::remove_all("test_db_large", ec);
		std::filesystem::remove_all("test_db_multi_large", ec);
	}
};

TEST_F(DBTest, BasicPutGetDelete)
{
	auto res = DB::Open("test_db");
	ASSERT_TRUE(res.has_value());

	auto db = std::move(res.value());

	// Put
	Status s1 = db->Put("key1", "value1");
	EXPECT_TRUE(s1.ok()) << "Put should succeed: " << s1.ToString();

	// Get
	std::string value;
	auto s2 = db->Get("key1");
	ASSERT_TRUE(s2.has_value()) << "Get should succeed";
	value = s2.value();
	EXPECT_EQ("value1", value) << "Value should match";

	// Delete
	Status s3 = db->Delete("key1");
	EXPECT_TRUE(s3.ok()) << "Delete should succeed: " << s3.ToString();

	// Get after delete
	std::string value2;
	auto s4 = db->Get("key1");
	EXPECT_TRUE(s4.error().IsNotFound()) << "Get should return NotFound after delete";
}

TEST_F(DBTest, BatchWrite)
{
	auto res = DB::Open("test_db");
	ASSERT_TRUE(res.has_value());

	auto db = std::move(res.value());

	WriteBatch batch;
	batch.Put("batch_key1", "batch_value1");
	batch.Put("batch_key2", "batch_value2");
	batch.Delete("key1");

	Status s = db->Write(batch);
	EXPECT_TRUE(s.ok()) << "Batch write should succeed: " << s.ToString();

	auto s1 = db->Get("batch_key1");
	ASSERT_TRUE(s1.has_value());
	EXPECT_EQ("batch_value1", s1.value());

	auto s2 = db->Get("batch_key2");
	ASSERT_TRUE(s2.has_value());
	EXPECT_EQ("batch_value2", s2.value());
}

TEST_F(DBTest, Recovery)
{
	{
		auto res = DB::Open("test_db");
		ASSERT_TRUE(res.has_value());

		auto db = std::move(res.value());
		Status s = db->Put("persistent_key", "persistent_value");
		ASSERT_TRUE(s.ok()) << s.ToString();
	}

	{
		auto res = DB::Open("test_db");
		ASSERT_TRUE(res.has_value());

		auto db = std::move(res.value());
		auto s = db->Get("persistent_key");
		ASSERT_TRUE(s.has_value());
		EXPECT_EQ("persistent_value", s.value()) << "Recovered value should match";
	}
}

TEST_F(DBTest, LargeValueFragmentation)
{
	auto res = DB::Open("test_db_large");
	ASSERT_TRUE(res.has_value());

	auto db = std::move(res.value());

	// Create a large value that will require fragmentation (> 32KB)
	std::string large_value(40000, 'X'); // 40KB of 'X'

	// Put the large value
	Status s1 = db->Put("large_key", large_value);
	ASSERT_TRUE(s1.ok()) << "Put large value should succeed: " << s1.ToString();

	// Get it back
	auto s2 = db->Get("large_key");
	ASSERT_TRUE(s2.has_value());
	EXPECT_EQ(large_value, s2.value()) << "Large value should match after fragmentation";

	// Test recovery with large value
	db.reset(); // Close DB

	res = DB::Open("test_db_large");
	ASSERT_TRUE(res.has_value());

	db = std::move(res.value());
	auto s3 = db->Get("large_key");
	ASSERT_TRUE(s3.has_value());
	EXPECT_EQ(large_value, s3.value()) << "Recovered large value should match";
}

TEST_F(DBTest, MultipleLargeRecords)
{
	auto res = DB::Open("test_db_multi_large");
	ASSERT_TRUE(res.has_value());

	auto db = std::move(res.value());

	// Write multiple large values
	std::string value1(50000, 'A'); // 50KB
	std::string value2(35000, 'B'); // 35KB
	std::string value3(45000, 'C'); // 45KB

	Status s1 = db->Put("key1", value1);
	Status s2 = db->Put("key2", value2);
	Status s3 = db->Put("key3", value3);

	ASSERT_TRUE(s1.ok()) << s1.ToString();
	ASSERT_TRUE(s2.ok()) << s2.ToString();
	ASSERT_TRUE(s3.ok()) << s3.ToString();

	// Verify all values
	auto r1 = db->Get("key1");
	auto r2 = db->Get("key2");
	auto r3 = db->Get("key3");

	ASSERT_TRUE(r1.has_value());
	ASSERT_TRUE(r2.has_value());
	ASSERT_TRUE(r3.has_value());

	EXPECT_EQ(value1, r1.value());
	EXPECT_EQ(value2, r2.value());
	EXPECT_EQ(value3, r3.value());
}

TEST_F(DBTest, Iterator)
{
	Options options;
	options.create_if_missing = true;
	options.write_buffer_size = 256;
	auto res = DB::Open(options, "test_db");
	ASSERT_TRUE(res.has_value());

	auto db = std::move(res.value());

	ASSERT_TRUE(db->Put("b", "2").ok());
	ASSERT_TRUE(db->Put("a", "1").ok());
	ASSERT_TRUE(db->Put("c", "3").ok());
	ASSERT_TRUE(db->Delete("b").ok());

	ReadOptions ro;
	std::unique_ptr<Iterator> it(db->NewIterator(ro));
	it->SeekToFirst();

	ASSERT_TRUE(it->Valid());
	EXPECT_EQ(it->key().ToString(), "a");
	EXPECT_EQ(it->value().ToString(), "1");

	it->Next();
	ASSERT_TRUE(it->Valid());
	EXPECT_EQ(it->key().ToString(), "c");
	EXPECT_EQ(it->value().ToString(), "3");

	it->Next();
	EXPECT_FALSE(it->Valid());
	EXPECT_TRUE(it->status().ok()) << it->status().ToString();
}

int main(int argc, char** argv)
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
