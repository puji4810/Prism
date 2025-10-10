#include "db.h"
#include "write_batch.h"

#include <gtest/gtest.h>

using namespace prism;

TEST(DBTest, BasicPutGetDelete)
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

TEST(DBTest, BatchWrite)
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

TEST(DBTest, Recovery)
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

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}