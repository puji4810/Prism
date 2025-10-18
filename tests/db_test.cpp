#include "db.h"
#include "write_batch.h"

#include <gtest/gtest.h>
#include <filesystem>

using namespace prism;

// Test fixture for DB tests
class DBTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any existing test databases before each test
        std::filesystem::remove("test_db");
        std::filesystem::remove("test_db_large");
        std::filesystem::remove("test_db_multi_large");
    }
    
    void TearDown() override {
        // Clean up after each test
        std::filesystem::remove("test_db");
        std::filesystem::remove("test_db_large");
        std::filesystem::remove("test_db_multi_large");
    }
};

TEST_F(DBTest, BasicPutGetDelete)
{
    auto db = DB::Open("test_db");
    
    // Put
    auto s1 = db->Put("key1", "value1");
    EXPECT_TRUE(s1.has_value()) << "Put should succeed";
    
    // Get
    auto r1 = db->Get("key1");
    ASSERT_TRUE(r1.has_value()) << "Get should succeed";
    EXPECT_EQ("value1", *r1) << "Value should match";
    
    // Delete
    auto s2 = db->Delete("key1");
    EXPECT_TRUE(s2.has_value()) << "Delete should succeed";
    
    // Get after delete
    auto r2 = db->Get("key1");
    EXPECT_FALSE(r2.has_value()) << "Get should fail after delete";
}

TEST_F(DBTest, BatchWrite)
{
    auto db = DB::Open("test_db");
    
    WriteBatch batch;
    batch.Put("batch_key1", "batch_value1");
    batch.Put("batch_key2", "batch_value2");
    batch.Delete("key1");
    
    auto s = db->Write(batch);
    EXPECT_TRUE(s.has_value()) << "Batch write should succeed";
    
    auto r1 = db->Get("batch_key1");
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ("batch_value1", *r1);
    
    auto r2 = db->Get("batch_key2");
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ("batch_value2", *r2);
}

TEST_F(DBTest, Recovery)
{
    {
        auto db = DB::Open("test_db");
        db->Put("persistent_key", "persistent_value");
    }
    
    {
        auto db = DB::Open("test_db");
        auto r = db->Get("persistent_key");
        ASSERT_TRUE(r.has_value()) << "Should recover data";
        EXPECT_EQ("persistent_value", *r) << "Recovered value should match";
    }
}

TEST_F(DBTest, LargeValueFragmentation)
{
    auto db = DB::Open("test_db_large");
    
    // Create a large value that will require fragmentation (> 32KB)
    std::string large_value(40000, 'X');  // 40KB of 'X'
    
    // Put the large value
    auto s1 = db->Put("large_key", large_value);
    ASSERT_TRUE(s1.has_value()) << "Put large value should succeed";
    
    // Get it back
    auto r1 = db->Get("large_key");
    ASSERT_TRUE(r1.has_value()) << "Get large value should succeed";
    EXPECT_EQ(large_value, *r1) << "Large value should match after fragmentation";
    
    // Test recovery with large value
    db.reset();  // Close DB
    
    db = DB::Open("test_db_large");
    auto r2 = db->Get("large_key");
    ASSERT_TRUE(r2.has_value()) << "Should recover large value";
    EXPECT_EQ(large_value, *r2) << "Recovered large value should match";
}

TEST_F(DBTest, MultipleLargeRecords)
{
    auto db = DB::Open("test_db_multi_large");
    
    // Write multiple large values
    std::string value1(50000, 'A');  // 50KB
    std::string value2(35000, 'B');  // 35KB
    std::string value3(45000, 'C');  // 45KB
    
    auto s1 = db->Put("key1", value1);
    auto s2 = db->Put("key2", value2);
    auto s3 = db->Put("key3", value3);
    
    ASSERT_TRUE(s1.has_value());
    ASSERT_TRUE(s2.has_value());
    ASSERT_TRUE(s3.has_value());
    
    // Verify all values
    auto r1 = db->Get("key1");
    auto r2 = db->Get("key2");
    auto r3 = db->Get("key3");
    
    ASSERT_TRUE(r1.has_value());
    ASSERT_TRUE(r2.has_value());
    ASSERT_TRUE(r3.has_value());
    
    EXPECT_EQ(value1, *r1);
    EXPECT_EQ(value2, *r2);
    EXPECT_EQ(value3, *r3);
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}