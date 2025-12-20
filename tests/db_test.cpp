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
	auto db = DB::Open("test_db");

	// Put
	Status s1 = db->Put("key1", "value1");
	EXPECT_TRUE(s1.ok()) << "Put should succeed: " << s1.ToString();

	// Get
	std::string value;
	Status s2 = db->Get("key1", &value);
	ASSERT_TRUE(s2.ok()) << "Get should succeed: " << s2.ToString();
	EXPECT_EQ("value1", value) << "Value should match";

	// Delete
	Status s3 = db->Delete("key1");
	EXPECT_TRUE(s3.ok()) << "Delete should succeed: " << s3.ToString();

	// Get after delete
	std::string value2;
	Status s4 = db->Get("key1", &value2);
	EXPECT_TRUE(s4.IsNotFound()) << "Get should return NotFound after delete";
}

TEST_F(DBTest, BatchWrite)
{
	auto db = DB::Open("test_db");

	WriteBatch batch;
	batch.Put("batch_key1", "batch_value1");
	batch.Put("batch_key2", "batch_value2");
	batch.Delete("key1");

	Status s = db->Write(batch);
	EXPECT_TRUE(s.ok()) << "Batch write should succeed: " << s.ToString();

	std::string value1;
	Status s1 = db->Get("batch_key1", &value1);
	ASSERT_TRUE(s1.ok()) << s1.ToString();
	EXPECT_EQ("batch_value1", value1);

	std::string value2;
	Status s2 = db->Get("batch_key2", &value2);
	ASSERT_TRUE(s2.ok()) << s2.ToString();
	EXPECT_EQ("batch_value2", value2);
}

TEST_F(DBTest, Recovery)
{
	{
		auto db = DB::Open("test_db");
		Status s = db->Put("persistent_key", "persistent_value");
		ASSERT_TRUE(s.ok()) << s.ToString();
	}

	{
		auto db = DB::Open("test_db");
		std::string value;
		Status s = db->Get("persistent_key", &value);
		ASSERT_TRUE(s.ok()) << "Should recover data: " << s.ToString();
		EXPECT_EQ("persistent_value", value) << "Recovered value should match";
	}
}

TEST_F(DBTest, LargeValueFragmentation)
{
	auto db = DB::Open("test_db_large");

	// Create a large value that will require fragmentation (> 32KB)
	std::string large_value(40000, 'X'); // 40KB of 'X'

	// Put the large value
	Status s1 = db->Put("large_key", large_value);
	ASSERT_TRUE(s1.ok()) << "Put large value should succeed: " << s1.ToString();

	// Get it back
	std::string retrieved_value;
	Status s2 = db->Get("large_key", &retrieved_value);
	ASSERT_TRUE(s2.ok()) << "Get large value should succeed: " << s2.ToString();
	EXPECT_EQ(large_value, retrieved_value) << "Large value should match after fragmentation";

	// Test recovery with large value
	db.reset(); // Close DB

	db = DB::Open("test_db_large");
	std::string recovered_value;
	Status s3 = db->Get("large_key", &recovered_value);
	ASSERT_TRUE(s3.ok()) << "Should recover large value: " << s3.ToString();
	EXPECT_EQ(large_value, recovered_value) << "Recovered large value should match";
}

TEST_F(DBTest, MultipleLargeRecords)
{
	auto db = DB::Open("test_db_multi_large");

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
	std::string retrieved1, retrieved2, retrieved3;
	Status r1 = db->Get("key1", &retrieved1);
	Status r2 = db->Get("key2", &retrieved2);
	Status r3 = db->Get("key3", &retrieved3);

	ASSERT_TRUE(r1.ok()) << r1.ToString();
	ASSERT_TRUE(r2.ok()) << r2.ToString();
	ASSERT_TRUE(r3.ok()) << r3.ToString();

	EXPECT_EQ(value1, retrieved1);
	EXPECT_EQ(value2, retrieved2);
	EXPECT_EQ(value3, retrieved3);
}

TEST_F(DBTest, Iterator)
{
	Options options;
	options.create_if_missing = true;
	options.write_buffer_size = 256;
	auto db = DB::Open(options, "test_db");
	ASSERT_NE(db, nullptr);

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
