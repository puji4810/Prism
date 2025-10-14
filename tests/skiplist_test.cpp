#ifndef SKIPLIST_TEST_CPP
#define SKIPLIST_TEST_CPP

#include "skiplist.h"
#include <gtest/gtest.h>
#include <set>
#include <random>

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
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp);
    
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
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp);

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
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp);
    
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
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp);
    
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
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp);
    
    list.Insert(5);
    EXPECT_DEATH(list.Insert(5), "");
}
#endif

TEST(SkipListTest, RandomHeightDistribution) {
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp);

	for (int i = 0; i < 10000; i++) {
        list.Insert(i);
    }
    
    EXPECT_TRUE(list.Contains(0));
    EXPECT_TRUE(list.Contains(5000));
    EXPECT_TRUE(list.Contains(9999));
    EXPECT_FALSE(list.Contains(10000));
}

TEST(SkipListTest, EdgeCases) {
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp);
    
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
    Comparator cmp;
    SkipList<Key, Comparator> list(cmp);
    
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

}  // namespace prism

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

#endif