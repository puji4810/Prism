#ifndef SKIPLIST_TEST_CPP
#define SKIPLIST_TEST_CPP

#include "skiplist.h"
#include "arena.h"
#include <gtest/gtest.h>
#include <set>
#include <random>
#include <algorithm>
#include <vector>

namespace prism {

struct Comparator {
    int operator()(const uint64_t& a, const uint64_t& b) const {
        if (a < b) return -1;
        else if (a > b) return +1;
        else return 0;
    }
};

using Key = uint64_t;

TEST(SkipListTest, Empty) {
    Arena arena;
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp, &arena);
    
    ASSERT_FALSE(list.Contains(10));

    SkipList<Key, Comparator>::Iterator iter(&list);
    ASSERT_FALSE(iter.Valid());
    
    iter.SeekToFirst();
    ASSERT_FALSE(iter.Valid());
    
    iter.Seek(100);
    ASSERT_FALSE(iter.Valid());
    
    iter.SeekToLast();
    ASSERT_FALSE(iter.Valid());
}

TEST(SkipListTest, InsertAndLookup) {
    const int N = 2000;
    const int R = 5000;
    std::mt19937 rnd(1000);
    std::set<Key> keys;
    Arena arena;
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp, &arena);

	for (int i = 0; i < N; i++) {
        Key key = rnd() % R;
        if (keys.insert(key).second) {
            list.Insert(key);
        }
    }

    for (int i = 0; i < R; i++) {
        if (list.Contains(i)) {
            ASSERT_EQ(keys.count(i), 1);
        } else {
            ASSERT_EQ(keys.count(i), 0);
        }
    }

    {
        SkipList<Key, Comparator>::Iterator iter(&list);
        ASSERT_FALSE(iter.Valid());

        iter.Seek(0);
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(*(keys.begin()), iter.key());

        iter.SeekToFirst();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(*(keys.begin()), iter.key());

        iter.SeekToLast();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(*(keys.rbegin()), iter.key());
    }

    for (int i = 0; i < R; i++) {
        SkipList<Key, Comparator>::Iterator iter(&list);
        iter.Seek(i);

        auto model_iter = keys.lower_bound(i);
        for (int j = 0; j < 3; j++) {
            if (model_iter == keys.end()) {
                ASSERT_FALSE(iter.Valid());
                break;
            } else {
                ASSERT_TRUE(iter.Valid());
                EXPECT_EQ(*model_iter, iter.key());
                ++model_iter;
                iter.Next();
            }
        }
    }

    {
        SkipList<Key, Comparator>::Iterator iter(&list);
        iter.SeekToLast();

        for (auto model_iter = keys.rbegin();
             model_iter != keys.rend(); ++model_iter) {
            ASSERT_TRUE(iter.Valid());
            EXPECT_EQ(*model_iter, iter.key());
            iter.Prev();
        }
        ASSERT_FALSE(iter.Valid());
    }
}

TEST(SkipListTest, Ordering) {
    Arena arena;
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp, &arena);
    
    std::vector<Key> keys = {5, 3, 8, 1, 9, 2, 7, 4, 6};
    for (Key k : keys) {
        list.Insert(k);
    }
    
    SkipList<Key, Comparator>::Iterator iter(&list);
    iter.SeekToFirst();
    
    Key expected = 1;
    while (iter.Valid()) {
        EXPECT_EQ(expected, iter.key());
        expected++;
        iter.Next();
    }
    EXPECT_EQ(expected, 10);
}

TEST(SkipListTest, SeekBehavior) {
    Arena arena;
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp, &arena);
    
    for (int i = 1; i <= 5; i++) {
        list.Insert(i * 10);
    }
    
    SkipList<Key, Comparator>::Iterator iter(&list);
    
    iter.Seek(30);
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(30, iter.key());
    
    iter.Seek(25);
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(30, iter.key());
    
    iter.Seek(5);
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(10, iter.key());

	iter.Seek(100);
    ASSERT_FALSE(iter.Valid());
}

#ifndef NDEBUG
TEST(SkipListTest, NoDuplicates) {
    Arena arena;
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp, &arena);
    
    list.Insert(5);
    EXPECT_DEATH(list.Insert(5), "");
}
#endif

TEST(SkipListTest, RandomHeightDistribution) {
    Arena arena;
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp, &arena);

	for (int i = 0; i < 10000; i++) {
        list.Insert(i);
    }
    
    EXPECT_TRUE(list.Contains(0));
    EXPECT_TRUE(list.Contains(5000));
    EXPECT_TRUE(list.Contains(9999));
    EXPECT_FALSE(list.Contains(10000));
}

TEST(SkipListTest, EdgeCases) {
    Arena arena;
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp, &arena);
    
    list.Insert(42);
    EXPECT_TRUE(list.Contains(42));
    
    SkipList<Key, Comparator>::Iterator iter(&list);
    iter.SeekToFirst();
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(42, iter.key());
    
    iter.SeekToLast();
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(42, iter.key());
    
    iter.Prev();
    EXPECT_FALSE(iter.Valid());
}

TEST(SkipListTest, LargeDataset) {
    Arena arena;
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp, &arena);
    
    const int N = 100000;
    std::mt19937 rnd(42);
    std::set<Key> inserted;
    

    for (int i = 0; i < N; i++) {
        Key k = rnd();
        if (inserted.insert(k).second) {
            list.Insert(k);
        }
    }
    
    for (Key k : inserted) {
        ASSERT_TRUE(list.Contains(k));
    }
    
    SkipList<Key, Comparator>::Iterator iter(&list);
    iter.SeekToFirst();
    
    auto set_iter = inserted.begin();
    while (iter.Valid() && set_iter != inserted.end()) {
        EXPECT_EQ(*set_iter, iter.key());
        ++set_iter;
        iter.Next();
    }
    EXPECT_FALSE(iter.Valid());
    EXPECT_EQ(set_iter, inserted.end());
}

TEST(SkipListTest, STLIterator_RangeBasedFor) {
    Arena arena;
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp, &arena);
    
    std::vector<Key> keys = {1, 3, 5, 7, 9};
    for (Key k : keys) {
        list.Insert(k);
    }
    
    // range-based for loop
    std::vector<Key> result;
    for (const auto& k : list) {
        result.push_back(k);
    }
    
    EXPECT_EQ(keys, result);
}

// Test: Basic operations of standard library iterator
TEST(SkipListTest, STLIterator_BasicOps) {
    Arena arena;
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp, &arena);
    
    for (int i = 0; i < 10; i++) {
        list.Insert(i * 10);
    }
    
    // begin/end
    auto it = list.begin();
    ASSERT_NE(it, list.end());
    EXPECT_EQ(0, *it);
    
    // operator*
    EXPECT_EQ(0, *it);
    
    // operator++
    ++it;
    EXPECT_EQ(10, *it);
    
    // operator++(int)
    auto it2 = it++;
    EXPECT_EQ(10, *it2);
    EXPECT_EQ(20, *it);
    
    // operator!=
    EXPECT_NE(it, list.end());
}

// Test: STL iterator backward traversal
TEST(SkipListTest, STLIterator_Backward) {
    Arena arena;
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp, &arena);
    
    std::vector<Key> keys = {1, 3, 5, 7, 9};
    for (Key k : keys) {
        list.Insert(k);
    }
    
    // From last to first
    auto it = list.end();
    --it;  // Point to the last element
    EXPECT_EQ(9, *it);
    
    --it;
    EXPECT_EQ(7, *it);
    
    --it;
    EXPECT_EQ(5, *it);
}

// Test: STL algorithm compatibility
TEST(SkipListTest, STLIterator_Algorithms) {
    Arena arena;
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp, &arena);
    
    for (int i = 0; i < 10; i++) {
        list.Insert(i * 2);  // 0, 2, 4, 6, 8, 10, 12, 14, 16, 18
    }
    
    // std::find
    auto it = std::find(list.begin(), list.end(), 8);
    ASSERT_NE(it, list.end());
    EXPECT_EQ(8, *it);
    
    // std::count
    auto count = std::count(list.begin(), list.end(), 8);
    EXPECT_EQ(1, count);
    
    // std::distance
    auto dist = std::distance(list.begin(), list.end());
    EXPECT_EQ(10, dist);
    
    // std::find_if
    auto it2 = std::find_if(list.begin(), list.end(), 
                            [](Key k) { return k > 10; });
    ASSERT_NE(it2, list.end());
    EXPECT_EQ(12, *it2);
}

// Test: Reverse iterator
TEST(SkipListTest, STLIterator_ReverseIterator) {
    Arena arena;
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp, &arena);
    
    std::vector<Key> keys = {1, 3, 5, 7, 9};
    for (Key k : keys) {
        list.Insert(k);
    }
    
    // Use reverse iterator
    std::vector<Key> reversed;
    for (auto it = list.rbegin(); it != list.rend(); ++it) {
        reversed.push_back(*it);
    }
    
    std::vector<Key> expected = {9, 7, 5, 3, 1};
    EXPECT_EQ(expected, reversed);
}

// Test: iterator and Iterator coexist
TEST(SkipListTest, BothIteratorStyles) {
    Arena arena;
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp, &arena);
    
    for (int i = 1; i <= 5; i++) {
        list.Insert(i * 10);
    }
    
    // Use leveldb style Iterator
    SkipList<Key, Comparator>::Iterator ldb_iter(&list);
    ldb_iter.Seek(25);
    ASSERT_TRUE(ldb_iter.Valid());
    EXPECT_EQ(30, ldb_iter.key());
    
    // Use standard library style iterator
    auto stl_it = std::find(list.begin(), list.end(), 30);
    ASSERT_NE(stl_it, list.end());
    EXPECT_EQ(30, *stl_it);
    
    // Both styles should see the same data
    EXPECT_EQ(ldb_iter.key(), *stl_it);
}

}  // namespace prism

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

#endif