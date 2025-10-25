#include "memtable.h"
#include "dbformat.h"
#include "comparator.h"
#include <gtest/gtest.h>
#include <string>

namespace prism
{

	class MemTableTest: public ::testing::Test
	{
	protected:
		void SetUp() override
		{
			comparator_ = new InternalKeyComparator(BytewiseComparator());
			memtable_ = new MemTable(*comparator_);
			memtable_->Ref();
		}

		void TearDown() override
		{
			memtable_->Unref();
			delete comparator_;
		}

		void Add(SequenceNumber seq, ValueType type, const std::string& key, const std::string& value)
		{
			memtable_->Add(seq, type, Slice(key), Slice(value));
		}

		std::string Get(const std::string& key, SequenceNumber seq, Status* s)
		{
			LookupKey lkey(Slice(key), seq);
			std::string value;
			bool found = memtable_->Get(lkey, &value, s);
			if (found)
			{
				return value;
			}
			return "NOT_FOUND";
		}

		InternalKeyComparator* comparator_;
		MemTable* memtable_;
	};

	TEST_F(MemTableTest, EmptyTable)
	{
		Status s;
		std::string value = Get("foo", 100, &s);
		ASSERT_EQ("NOT_FOUND", value);
	}

	TEST_F(MemTableTest, SimpleInsertAndGet)
	{
		Add(100, kTypeValue, "foo", "bar");

		Status s;
		std::string value = Get("foo", 100, &s);
		ASSERT_EQ("bar", value);
		ASSERT_TRUE(s.ok());
	}

	TEST_F(MemTableTest, MultipleInserts)
	{
		Add(100, kTypeValue, "foo", "v1");
		Add(101, kTypeValue, "bar", "v2");
		Add(102, kTypeValue, "baz", "v3");

		Status s;
		ASSERT_EQ("v1", Get("foo", 100, &s));
		ASSERT_TRUE(s.ok());

		ASSERT_EQ("v2", Get("bar", 101, &s));
		ASSERT_TRUE(s.ok());

		ASSERT_EQ("v3", Get("baz", 102, &s));
		ASSERT_TRUE(s.ok());
	}

	TEST_F(MemTableTest, MultipleVersions)
	{
		// Insert multiple versions of the same key
		Add(100, kTypeValue, "foo", "v1");
		Add(200, kTypeValue, "foo", "v2");
		Add(300, kTypeValue, "foo", "v3");

		Status s;
		// Query with seq >= 300 should get v3
		ASSERT_EQ("v3", Get("foo", 300, &s));
		ASSERT_TRUE(s.ok());

		// Query with seq >= 200 but < 300 should get v2
		ASSERT_EQ("v2", Get("foo", 200, &s));
		ASSERT_TRUE(s.ok());

		// Query with seq >= 100 but < 200 should get v1
		ASSERT_EQ("v1", Get("foo", 100, &s));
		ASSERT_TRUE(s.ok());

		// Query with seq < 100 should not find anything
		ASSERT_EQ("NOT_FOUND", Get("foo", 99, &s));
	}

	TEST_F(MemTableTest, Deletion)
	{
		// Add a value
		Add(100, kTypeValue, "foo", "bar");

		Status s;
		ASSERT_EQ("bar", Get("foo", 100, &s));
		ASSERT_TRUE(s.ok());

		// Delete the key
		Add(200, kTypeDeletion, "foo", "");

		// Query after deletion should return NotFound
		s = Status::OK();
		std::string value = Get("foo", 200, &s);
		ASSERT_TRUE(s.IsNotFound());

		// Query before deletion should still see the old value
		s = Status::OK();
		ASSERT_EQ("bar", Get("foo", 150, &s));
		ASSERT_TRUE(s.ok());
	}

	TEST_F(MemTableTest, DeletionWithoutPriorValue)
	{
		// Delete a key that was never inserted
		Add(100, kTypeDeletion, "foo", "");

		Status s;
		std::string value = Get("foo", 100, &s);
		ASSERT_TRUE(s.IsNotFound());
	}

	TEST_F(MemTableTest, UpdateAfterDeletion)
	{
		// Insert, delete, then insert again
		Add(100, kTypeValue, "foo", "v1");
		Add(200, kTypeDeletion, "foo", "");
		Add(300, kTypeValue, "foo", "v2");

		Status s;
		// Latest query should get v2
		ASSERT_EQ("v2", Get("foo", 300, &s));
		ASSERT_TRUE(s.ok());

		// Query at deletion point
		s = Status::OK();
		Get("foo", 200, &s);
		ASSERT_TRUE(s.IsNotFound());

		// Query before deletion
		s = Status::OK();
		ASSERT_EQ("v1", Get("foo", 150, &s));
		ASSERT_TRUE(s.ok());
	}

	TEST_F(MemTableTest, EmptyValue)
	{
		// Insert with empty value
		Add(100, kTypeValue, "empty_key", "");

		Status s;
		std::string value = Get("empty_key", 100, &s);
		ASSERT_EQ("", value);
		ASSERT_TRUE(s.ok());
	}

	TEST_F(MemTableTest, LargeValue)
	{
		// Insert a large value
		std::string large_value(10000, 'x');
		Add(100, kTypeValue, "large", large_value);

		Status s;
		std::string value = Get("large", 100, &s);
		ASSERT_EQ(large_value, value);
		ASSERT_TRUE(s.ok());
	}

	TEST_F(MemTableTest, ManyKeys)
	{
		// Insert many different keys
		const int N = 1000;
		for (int i = 0; i < N; i++)
		{
			std::string key = "key" + std::to_string(i);
			std::string value = "value" + std::to_string(i);
			Add(100 + i, kTypeValue, key, value);
		}

		// Verify all keys
		Status s;
		for (int i = 0; i < N; i++)
		{
			std::string key = "key" + std::to_string(i);
			std::string expected = "value" + std::to_string(i);
			ASSERT_EQ(expected, Get(key, 100 + i, &s));
			ASSERT_TRUE(s.ok());
		}
	}

	TEST_F(MemTableTest, ReferenceCount)
	{
		// Create a MemTable and verify reference counting
		InternalKeyComparator cmp(BytewiseComparator());
		MemTable* mem = new MemTable(cmp);

		// Initially refs = 0
		mem->Ref(); // refs = 1
		mem->Add(100, kTypeValue, Slice("foo"), Slice("bar"));

		mem->Ref(); // refs = 2
		mem->Unref(); // refs = 1

		// Verify data is still accessible
		LookupKey lkey(Slice("foo"), 100);
		std::string value;
		Status s;
		bool found = mem->Get(lkey, &value, &s);
		ASSERT_TRUE(found);
		ASSERT_EQ("bar", value);

		mem->Unref(); // refs = 0, should delete
		// After this point, mem is deleted and should not be accessed
	}

	TEST_F(MemTableTest, ApproximateMemoryUsage)
	{
		size_t initial = memtable_->ApproximateMemoryUsage();

		// Add some data (Arena allocates in 4KB blocks, so small inserts may not grow immediately)
		Add(100, kTypeValue, "key1", "value1");
		size_t after_one = memtable_->ApproximateMemoryUsage();
		// Memory should be at least initial (may grow or stay same depending on block allocation)
		ASSERT_GE(after_one, initial);

		// Add more data
		for (int i = 0; i < 100; i++)
		{
			std::string key = "key" + std::to_string(i);
			std::string value = "value" + std::to_string(i);
			Add(100 + i, kTypeValue, key, value);
		}
		size_t after_many = memtable_->ApproximateMemoryUsage();
		ASSERT_GT(after_many, initial);

		// Add a large value that definitely needs a new block
		std::string large_value(5000, 'x');
		Add(200, kTypeValue, "key_large", large_value);
		size_t after_large = memtable_->ApproximateMemoryUsage();
		ASSERT_GT(after_large, after_many); // Should definitely grow with large value
	}

	TEST_F(MemTableTest, SequenceOrdering)
	{
		// Add keys in non-sequential order
		Add(300, kTypeValue, "foo", "v3");
		Add(100, kTypeValue, "foo", "v1");
		Add(200, kTypeValue, "foo", "v2");

		Status s;
		// Should still get correct versions based on sequence
		ASSERT_EQ("v3", Get("foo", 300, &s));
		ASSERT_EQ("v2", Get("foo", 250, &s));
		ASSERT_EQ("v1", Get("foo", 150, &s));
	}

	TEST_F(MemTableTest, KeyOrdering)
	{
		// Add keys in non-lexicographic order
		Add(100, kTypeValue, "zebra", "v1");
		Add(101, kTypeValue, "apple", "v2");
		Add(102, kTypeValue, "middle", "v3");

		Status s;
		ASSERT_EQ("v1", Get("zebra", 100, &s));
		ASSERT_EQ("v2", Get("apple", 101, &s));
		ASSERT_EQ("v3", Get("middle", 102, &s));
	}

	TEST_F(MemTableTest, BinaryKeysAndValues)
	{
		// Test with binary data (including null bytes)
		std::string binary_key = std::string("key\0with\0nulls", 14);
		std::string binary_value = std::string("val\0ue\0", 7);

		Add(100, kTypeValue, binary_key, binary_value);

		Status s;
		std::string value = Get(binary_key, 100, &s);
		ASSERT_EQ(binary_value, value);
		ASSERT_TRUE(s.ok());
	}

} // namespace prism

int main(int argc, char** argv)
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
