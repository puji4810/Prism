#include "cache.h"

#include <atomic>
#include <barrier>
#include <gtest/gtest.h>
#include <memory>
#include <thread>
#include <vector>

using namespace prism;

namespace
{
	struct CacheValue
	{
		std::atomic<int>* deletes;
		int payload;
	};

	void DeleteCacheValue(const Slice&, void* value)
	{
		auto* cache_value = static_cast<CacheValue*>(value);
		cache_value->deletes->fetch_add(1, std::memory_order_relaxed);
		delete cache_value;
	}
}

TEST(CacheTest, ConcurrentHotEntryLookupAndRelease)
{
	std::unique_ptr<Cache> cache(NewLRUCache(1024));
	std::atomic<int> deletes{ 0 };
	auto* inserted = cache->Insert(Slice("hot"), new CacheValue{ &deletes, 42 }, 1, &DeleteCacheValue);
	cache->Release(inserted);

	constexpr int kThreads = 24;
	constexpr int kIterations = 20000;
	std::barrier start(kThreads);
	std::atomic<int> failures{ 0 };
	std::vector<std::jthread> threads;
	threads.reserve(kThreads);
	for (int thread = 0; thread < kThreads; ++thread)
	{
		threads.emplace_back([&] {
			start.arrive_and_wait();
			for (int i = 0; i < kIterations; ++i)
			{
				auto* handle = cache->Lookup(Slice("hot"));
				if (handle == nullptr || static_cast<CacheValue*>(cache->Value(handle))->payload != 42)
				{
					failures.fetch_add(1, std::memory_order_relaxed);
				}
				if (handle != nullptr)
				{
					cache->Release(handle);
				}
			}
		});
	}
	threads.clear();

	EXPECT_EQ(failures.load(std::memory_order_relaxed), 0);
	EXPECT_EQ(deletes.load(std::memory_order_relaxed), 0);
	cache->Erase(Slice("hot"));
	EXPECT_EQ(deletes.load(std::memory_order_relaxed), 1);
}

TEST(CacheTest, EraseDefersDeletionUntilConcurrentHandlesRelease)
{
	std::unique_ptr<Cache> cache(NewLRUCache(1024));
	std::atomic<int> deletes{ 0 };
	auto* inserted = cache->Insert(Slice("held"), new CacheValue{ &deletes, 7 }, 1, &DeleteCacheValue);
	cache->Release(inserted);

	constexpr int kHandles = 24;
	std::vector<Cache::Handle*> handles;
	for (int i = 0; i < kHandles; ++i)
	{
		handles.push_back(cache->Lookup(Slice("held")));
		ASSERT_NE(handles.back(), nullptr);
	}

	cache->Erase(Slice("held"));
	EXPECT_EQ(cache->Lookup(Slice("held")), nullptr);
	EXPECT_EQ(deletes.load(std::memory_order_relaxed), 0);

	std::vector<std::jthread> releasers;
	for (auto* handle : handles)
	{
		releasers.emplace_back([&, handle] { cache->Release(handle); });
	}
	releasers.clear();
	EXPECT_EQ(deletes.load(std::memory_order_relaxed), 1);
}

TEST(CacheTest, EmptyKeyHasValidHandleStorage)
{
	std::unique_ptr<Cache> cache(NewLRUCache(1024));
	std::atomic<int> deletes{ 0 };
	const Slice empty_key("", 0);
	auto* inserted = cache->Insert(empty_key, new CacheValue{ &deletes, 9 }, 1, &DeleteCacheValue);
	ASSERT_NE(inserted, nullptr);
	cache->Release(inserted);
	auto* found = cache->Lookup(empty_key);
	ASSERT_NE(found, nullptr);
	EXPECT_EQ(static_cast<CacheValue*>(cache->Value(found))->payload, 9);
	cache->Release(found);
	cache->Erase(empty_key);
	EXPECT_EQ(deletes.load(std::memory_order_relaxed), 1);
}
