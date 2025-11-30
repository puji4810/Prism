#include "cache.h"
#include "options.h"
#include "slice.h"
#include "port/thread_annotations.h"
#include "hash.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <sys/types.h>

namespace prism
{
	Cache::~Cache() = default;

	// LRUHandle represents a single entry managed by the LRU cache.
	//
	// Lifetime / ownership
	// - The cache itself holds at most one reference to an entry while it is
	//   present in the hash table and on one of the internal lists.
	// - Clients obtain additional references through Insert()/Lookup() and must
	//   eventually release them via Release()/Unref().
	// - The flag `in_cache` becomes false only when the entry is logically removed
	//   from the cache: via Erase(), via Insert() with a duplicate key that
	//   overwrites this entry, or during cache destruction.
	// - Once `in_cache == false`, the entry must not appear on either the LRU list
	//   or the in-use list; it is only kept alive by outstanding client references.
	//
	// Logical states
	// Conceptually an entry can be in one of three states:
	//  1) in_cache == true  and externally referenced
	//     - The entry is on the "in-use" list.
	//  2) in_cache == true  and not externally referenced
	//     - The entry is on the LRU list and is eligible for eviction.
	//  3) in_cache == false and externally referenced
	//     - The entry is on neither list and will be destroyed once `refs` drops to 0.
	//
	// Invariants
	// - Every entry with in_cache == true is on exactly one of the two lists
	//   (in-use or LRU), never both and never neither.
	// - Every entry with in_cache == false is on neither list.
	// - The LRU list is ordered by recency of use; the in-use list has no
	//   particular ordering semantics.
	//
	// Ref() / Unref() transitions
	// - Ref(): called when an external reference is acquired. If this is the first
	//   external reference for an entry that is currently on the LRU list, the
	//   entry is moved from the LRU list to the in-use list.
	// - Unref(): decrements `refs`. When `refs` reaches 0:
	//   * If in_cache == true, the entry is placed onto the LRU list.
	//   * If in_cache == false, the entry is destroyed via its `deleter`.
	struct LRUHandle
	{
		void* value;
		using Deleter = void (*)(const Slice&, void* value);
		Deleter deleter;
		LRUHandle* next_hash;
		LRUHandle* next;
		LRUHandle* prev;

		size_t charge;
		size_t key_length;
		bool in_cache;
		uint32_t refs;
		uint32_t hash;
		char key_data[1];

		Slice key() const
		{
			// next == this only when LRU handle is the head of an empty list
			// List head never have meaningful keys
			assert(next != this);
			return Slice(key_data, key_length);
		}
	};

	class HandleTable
	{
	public:
		HandleTable()
		    : length_(0)
		    , elems_(0)
		    , list_(nullptr)
		{
			Resize();
		}

		~HandleTable() { delete[] list_; }

		LRUHandle* Lookup(const Slice& key, uint32_t hash) { return *FindPointer(key, hash); }

		LRUHandle* Insert(LRUHandle* h)
		{
			LRUHandle** ptr = FindPointer(h->key(), h->hash);
			LRUHandle* old = *ptr;
			h->next_hash = (old == nullptr ? nullptr : old->next_hash);
			*ptr = h;

			if (old == nullptr) // insert a new node
			{
				elems_++;
				if (elems_ > length_)
				{
					Resize();
				}
			}
			return old;
		}

		LRUHandle* Remove(const Slice& key, uint32_t hash)
		{
			LRUHandle** ptr = FindPointer(key, hash);
			LRUHandle* result = *ptr;
			if (result != nullptr)
			{
				*ptr = result->next_hash;
				--elems_;
			}
			return result;
		}

	private:
		uint32_t length_;
		uint32_t elems_;
		LRUHandle** list_;

		LRUHandle** FindPointer(const Slice& key, uint32_t hash)
		{
			LRUHandle** ptr = &list_[hash & (length_ - 1)];
			// look through the list for the (key, value)
			// - *ptr == nullptr : the list does't have the key, just return the position for later insertion
			// - (*ptr)->hash == has && (*ptr)->key == key : find the position, return ptr
			while (*ptr != nullptr && ((*ptr)->hash != hash || (*ptr)->key() != key))
			{
				ptr = &(*ptr)->next_hash;
			}
			return ptr;
		}

		void Resize()
		{
			uint32_t new_length = 4;
			while (new_length < elems_)
			{
				new_length *= 2;
			}
			LRUHandle** new_list = new LRUHandle*[new_length];
			memset(new_list, 0, sizeof(new_list[0]) * new_length);
			uint32_t count = 0;
			for (uint32_t i = 0; i < length_; ++i)
			{
				LRUHandle* h = list_[i];
				// for every node, head insert the h into the backet in the new_list
				while (h != nullptr)
				{
					LRUHandle* next = h->next_hash;
					uint32_t hash = h->hash;
					LRUHandle** ptr = &new_list[hash & (new_length - 1)];
					h->next_hash = *ptr;
					*ptr = h;
					h = next;
					count++;
				}
			}
			assert(elems_ == count);
			delete[] list_;
			list_ = new_list;
			length_ = new_length;
		}
	};

	class LRUCache
	{
	public:
		LRUCache();
		~LRUCache();

		void SetCapacity(size_t capacity) { capacity_ = capacity; }
		Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value, size_t charge, LRUHandle::Deleter deleter);
		Cache::Handle* Lookup(const Slice& key, uint32_t hash);
		void Release(Cache::Handle* handle);
		void Erase(const Slice& key, uint32_t hash);
		void Prune();
		size_t TotalCharge() const
		{
			std::lock_guard<std::mutex> guard(mutex_);
			return usage_;
		}

	private:
		size_t capacity_;

		mutable std::mutex mutex_;

		size_t usage_ GUARDED_BY(mutex_);
		LRUHandle lru_ GUARDED_BY(mutex_);
		LRUHandle in_use_ GUARDED_BY(mutex_);
		HandleTable table_ GUARDED_BY(mutex_);

		void LRU_Remove(LRUHandle* e);
		void LRU_Append(LRUHandle* list, LRUHandle* e);
		void Ref(LRUHandle* e);
		void Unref(LRUHandle* e);
		bool FinishErase(LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
	};

	LRUCache::LRUCache()
	    : capacity_(0)
	    , usage_(0)
	{
		lru_.next = lru_.prev = &lru_;
		in_use_.next = in_use_.prev = &in_use_;
	}

	LRUCache::~LRUCache()
	{
		assert(in_use_.next == &in_use_); // make sure all handle is released
		for (LRUHandle* e = lru_.next; e != &lru_;) // go through the list
		{
			LRUHandle* next = e->next;
			assert(e->in_cache);
			e->in_cache = false;
			assert(e->refs == 1); // Invariant of lru_ list.
			Unref(e);
			e = next;
		}
	}

	void LRUCache::Ref(LRUHandle* e)
	{
		if (e->refs == 1 && e->in_cache)
		{ // If on lru_ list, move to in_use_ list.
			LRU_Remove(e);
			LRU_Append(&in_use_, e);
		}
		e->refs++;
	}

	void LRUCache::Unref(LRUHandle* e)
	{
		assert(e->refs > 0);
		e->refs--;
		if (e->refs == 0)
		{ // Deallocate.
			assert(!e->in_cache);
			(*e->deleter)(e->key(), e->value);
			free(e);
		}
		else if (e->in_cache && e->refs == 1)
		{
			// No longer in use; move to lru_ list.
			LRU_Remove(e);
			LRU_Append(&lru_, e);
		}
	}

	void LRUCache::LRU_Remove(LRUHandle* e)
	{
		e->next->prev = e->prev;
		e->prev->next = e->next;
	}

	void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e)
	{
		// Make "e" newest entry by inserting just before *list
		e->next = list;
		e->prev = list->prev;
		e->prev->next = e;
		e->next->prev = e;
	}

	Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash)
	{
		std::lock_guard<std::mutex> guard(mutex_);
		LRUHandle* e = table_.Lookup(key, hash);
		if (e != nullptr)
		{
			Ref(e);
		}
		return reinterpret_cast<Cache::Handle*>(e);
	}

	void LRUCache::Release(Cache::Handle* handle)
	{
		std::lock_guard<std::mutex> guard(mutex_);
		Unref(reinterpret_cast<LRUHandle*>(handle));
	}

	Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value, size_t charge, LRUHandle::Deleter deleter)
	{
		std::lock_guard<std::mutex> guard(mutex_);

		LRUHandle* e = reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));
		*e = LRUHandle{
			.value = value, .deleter = deleter, .charge = charge, .key_length = key.size(), .in_cache = false, .refs = 1, .hash = hash
		};
		std::memcpy(e->key_data, key.data(), key.size());

		if (capacity_ > 0)
		{
			// push into the LRU
			e->refs++;
			e->in_cache = true;
			LRU_Append(&in_use_, e);
			usage_ += charge;
			FinishErase(table_.Insert(e));
		}
		else
		{
			// don't cache. (capacity_==0 is supported and turns off caching.)
			// next is read by key() in an assert, so it must be initialized
			e->next = nullptr;
		}

		// free nodes
		while (usage_ > capacity_ && lru_.next != &lru_)
		{
			LRUHandle* old = lru_.next;
			assert(old->refs == 1); // every node in lru_ list should have refs == 1
			auto erased = FinishErase(table_.Remove(old->key(), old->hash));
			if (!erased)
			{
				assert(erased); // failed, something goes wrong
			}
		}

		return reinterpret_cast<Cache::Handle*>(e);
	}

	// If e != nullptr, finish removing *e from the cache; it has already been
	// removed from the hash table.  Return whether e != nullptr.
	bool LRUCache::FinishErase(LRUHandle* e)
	{
		if (e != nullptr)
		{
			assert(e->in_cache);
			LRU_Remove(e);
			e->in_cache = false;
			usage_ -= e->charge;
			Unref(e);
		}
		return e != nullptr;
	}

	void LRUCache::Erase(const Slice& key, uint32_t hash)
	{
		std::lock_guard<std::mutex> guard(mutex_);
		FinishErase(table_.Remove(key, hash));
	}

	void LRUCache::Prune()
	{
		std::lock_guard<std::mutex> guard(mutex_);
		while (lru_.next != &lru_)
		{
			LRUHandle* e = lru_.next;
			assert(e->refs == 1);
			bool erased = FinishErase(table_.Remove(e->key(), e->hash));
			if (!erased)
			{ // to avoid unused variable when compiled NDEBUG
				assert(erased);
			}
		}
	}

	static constexpr int kNumShardBits = 4;
	static constexpr int kNumShards = 1 << kNumShardBits;

	class ShardedLRUCache: public Cache
	{
	private:
		LRUCache shard_[kNumShards];
		std::mutex id_mutex_;
		uint64_t last_id_;

		static inline uint32_t HashSlice(const Slice& s) { return Hash(s.data(), s.size(), 0); }

		static inline uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

	public:
		explicit ShardedLRUCache(size_t capacity)
		    : last_id_(0)
		{
			const size_t per_shard = (capacity + kNumShards - 1) / kNumShards;
			for (int s = 0; s < kNumShards; ++s)
			{
				shard_[s].SetCapacity(per_shard);
			}
		}

		~ShardedLRUCache() override {}

		Handle* Insert(const Slice& key, void* value, size_t charge, LRUHandle::Deleter deleter) override
		{
			const uint32_t hash = HashSlice(key);
			return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter); // choose a shard_ slice
		}

		Handle* Lookup(const Slice& key) override
		{
			const uint32_t hash = HashSlice(key);
			return shard_[Shard(hash)].Lookup(key, hash);
		}
		void Release(Handle* handle) override
		{
			LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
			shard_[Shard(h->hash)].Release(handle);
		}
		void Erase(const Slice& key) override
		{
			const uint32_t hash = HashSlice(key);
			shard_[Shard(hash)].Erase(key, hash);
		}
		void* Value(Handle* handle) override { return reinterpret_cast<LRUHandle*>(handle)->value; }
		uint64_t NewId() override
		{
			std::lock_guard<std::mutex> guard(id_mutex_);
			return ++(last_id_);
		}
		void Prune() override
		{
			for (int s = 0; s < kNumShards; s++)
			{
				shard_[s].Prune();
			}
		}
		size_t TotalCharge() const override
		{
			size_t total = 0;
			for (int s = 0; s < kNumShards; s++)
			{
				total += shard_[s].TotalCharge();
			}
			return total;
		}
	};

	Cache* NewLRUCache(size_t capacity) { return new ShardedLRUCache(capacity); }
};
