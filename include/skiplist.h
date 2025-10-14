#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <atomic>
#include <cassert>
#include <vector>
#include "random.h"

namespace prism
{
	template <typename Key, class Comparator>
	struct SkipList
	{
	public:
		explicit SkipList(Comparator cmp);
		SkipList(const SkipList&) = delete;
		SkipList& operator=(const SkipList&) = delete;
		SkipList(SkipList&&) = delete;
		SkipList& operator=(SkipList&&) = delete;
		~SkipList()
		{
			for (char* block : blocks_)
			{
				delete[] block;
			}
		}

		void Insert(const Key& key);
		bool Contains(const Key& key) const;

		class Iterator;
		struct Node;

		// Iterator for the skip list.
		class Iterator
		{
		public:
			// Initialize an iterator over the specified list.
			// The returned iterator is not valid.
			explicit Iterator(const SkipList* list);

			// Returns true iff the iterator is positioned at a valid node.
			bool Valid() const;

			// Returns the key at the current position.
			// REQUIRES: Valid()
			const Key& key() const;

			// Advances to the next position.
			// REQUIRES: Valid()
			void Next();

			// Advances to the previous position.
			// REQUIRES: Valid()
			void Prev();

			// Advance to the first entry with a key >= target
			void Seek(const Key& target);

			// Position at the first entry in list.
			// Final state of iterator is Valid() iff list is not empty.
			void SeekToFirst();

			// Position at the last entry in list.
			// Final state of iterator is Valid() iff list is not empty.
			void SeekToLast();

		private:
			const SkipList* list_;
			Node* node_;
			// Intentionally copyable
		};

		// Standard iterator for STL
		class iterator
		{
		public:
			using iterator_category = std::bidirectional_iterator_tag;
			using value_type = Key;
			using difference_type = std::ptrdiff_t;
			using pointer = const Key*;
			using reference = const Key&;

		iterator()
		    : node_(nullptr)
		    , list_(nullptr)
		{
		}
		
		iterator(const SkipList* list, Node* n)
		    : node_(n)
		    , list_(list)
		{
		}

			reference operator*() const
			{
				assert(node_ != nullptr);
				return node_->key;
			}
			pointer operator->() const { return &node_->key; }

			iterator& operator++()
			{
				assert(node_ != nullptr);
				node_ = node_->Next(0);
				return *this;
			}
			iterator operator++(int)
			{
				iterator tmp = *this;
				++(*this);
				return tmp;
			}

			iterator& operator--();

		bool operator==(const iterator& other) const { return node_ == other.node_; }
		bool operator!=(const iterator& other) const { return node_ != other.node_; }

	private:
		Node* node_;
		const SkipList* list_;
	};

	using const_iterator = iterator;
	using reverse_iterator = std::reverse_iterator<iterator>;
	
	iterator begin() const { return iterator(this, head_->Next(0)); }
	iterator end() const { return iterator(this, nullptr); }
	reverse_iterator rbegin() const { return reverse_iterator(end()); }
	reverse_iterator rend() const { return reverse_iterator(begin()); }

private:
	std::vector<char*> blocks_;
		Node* const head_;
		static const int kMaxHeight = 12;
		std::atomic<int> max_height_; // Used by Insert()
		Comparator const compare_;
		Random rnd_;

		Node* NewNode(const Key& key, int height);

		// Return true if a equals b.
		bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }
		// Return the maximum height of the skip list.
		inline int GetMaxHeight() const { return max_height_.load(std::memory_order_relaxed); }
		// Return the node with the greatest key greater than or equal to key.
		Node* GetNodeGreaterOrEqual(const Key& key, Node** prev) const;
		// Return the latest node with a key less than key.
		Node* GetNodeLessThan(const Key& key) const;
		// Return the last node in the list.
		Node* GetLastNode() const;
		// Return true if key is greater than the node's key.
		bool KeyIsAfterNode(const Key& key, Node* n) const;
	// Return a random height for a new node.
	int RandomHeight();
};

	template <typename Key, class Comparator>
	inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list)
	{
		list_ = list;
		node_ = nullptr;
	}

	template <typename Key, class Comparator>
	inline bool SkipList<Key, Comparator>::Iterator::Valid() const
	{
		return node_ != nullptr;
	}

	template <typename Key, class Comparator>
	inline const Key& SkipList<Key, Comparator>::Iterator::key() const
	{
		assert(Valid());
		return node_->key;
	}

	template <typename Key, class Comparator>
	inline void SkipList<Key, Comparator>::Iterator::Next()
	{
		assert(Valid());
		node_ = node_->Next(0);
	}

	template <typename Key, class Comparator>
	inline void SkipList<Key, Comparator>::Iterator::Prev()
	{
		assert(Valid());
		node_ = list_->GetNodeLessThan(node_->key);
		if (node_ == list_->head_)
		{
			node_ = nullptr;
		}
	}

	template <typename Key, class Comparator>
	inline typename SkipList<Key, Comparator>::iterator& SkipList<Key, Comparator>::iterator::operator--()
	{
		assert(list_ != nullptr);
		if (node_ == nullptr)
		{
			// From end() to the last element.
			node_ = list_->GetLastNode();
			if (node_ == list_->head_)
				node_ = nullptr;
		}
		else
		{
			node_ = list_->GetNodeLessThan(node_->key);
			if (node_ == list_->head_)
				node_ = nullptr;
		}
		return *this;
	}

	template <typename Key, class Comparator>
	inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target)
	{
		node_ = list_->GetNodeGreaterOrEqual(target, nullptr);
	}

	template <typename Key, class Comparator>
	inline void SkipList<Key, Comparator>::Iterator::SeekToFirst()
	{
		node_ = list_->head_->Next(0);
	}

	template <typename Key, class Comparator>
	inline void SkipList<Key, Comparator>::Iterator::SeekToLast()
	{
		node_ = list_->GetLastNode();
		if (node_ == list_->head_)
		{
			node_ = nullptr;
		}
	}

	template <typename Key, class Comparator>
	struct SkipList<Key, Comparator>::Node
	{
		explicit Node(const Key& k)
		    : key(k)
		{
		}

		Node* Next(int n)
		{
			assert(n >= 0);
			return next_[n].load(std::memory_order_acquire);
		}
		void SetNext(int n, Node* x)
		{
			assert(n >= 0);
			next_[n].store(x, std::memory_order_release);
		}

		Node* NoBarrier_Next(int n)
		{
			assert(n >= 0);
			return next_[n].load(std::memory_order_relaxed);
		}
		void NoBarrier_SetNext(int n, Node* x)
		{
			assert(n >= 0);
			next_[n].store(x, std::memory_order_relaxed);
		}

	public:
		const Key key;

	private:
		std::atomic<Node*> next_[1];
	};

	template <typename Key, class Comparator>
	SkipList<Key, Comparator>::SkipList(Comparator cmp)
	    : head_(NewNode(Key{}, kMaxHeight))
	    , max_height_(1)
	    , compare_(cmp)
	    , rnd_(0xdeadbeef)
	{
		for (int i = 0; i < kMaxHeight; i++)
		{
			head_->SetNext(i, nullptr);
		}
	}

	template <typename Key, class Comparator>
	typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::NewNode(const Key& key, int height)
	{
		char* const node_memory = new char[sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1)];
		blocks_.push_back(node_memory);
		return new (node_memory) Node(key);
	}

	template <typename Key, class Comparator>
	void SkipList<Key, Comparator>::Insert(const Key& key)
	{
		Node* prev[kMaxHeight];
		Node* x = GetNodeGreaterOrEqual(key, prev);

		assert(x == nullptr || !Equal(key, x->key));

		int height = RandomHeight();
		if (height > GetMaxHeight())
		{
			for (int i = GetMaxHeight(); i < height; i++)
			{
				prev[i] = head_;
			}
			max_height_.store(height, std::memory_order_relaxed);
		}
		x = NewNode(key, height);

		for (int i = 0; i < height; i++)
		{
			x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
			prev[i]->SetNext(i, x);
		}
	}

	template <typename Key, class Comparator>
	bool SkipList<Key, Comparator>::Contains(const Key& key) const
	{
		// Standard implementation
		// Node* x = head_;
		// int level = GetMaxHeight() - 1;
		// for (int i = level; i >= 0; i--)
		// {
		// 	while (x->Next(i) != nullptr && compare_(x->Next(i)->key, key) < 0)
		// 	{
		// 		x = x->Next(i);
		// 	}
		// }
		// return x->Next(0) != nullptr && Equal(x->Next(0)->key, key);
		Node* x = GetNodeGreaterOrEqual(key, nullptr);
		return x != nullptr && Equal(x->key, key);
	}

	template <typename Key, class Comparator>
	typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::GetNodeGreaterOrEqual(const Key& key, Node** prev) const
	{
		Node* x = head_;
		int level = GetMaxHeight() - 1;

		while (true)
		{
			Node* next = x->Next(level);
			if (KeyIsAfterNode(key, next))
			{
				x = next;
			}
			else
			{
				if (prev != nullptr) // For Insert()
					prev[level] = x;

				if (level == 0) // For Search()
					return next;
				else
					level--;
			}
		}
	}

	template <typename Key, class Comparator>
	typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::GetNodeLessThan(const Key& key) const
	{
		Node* x = head_;
		int level = GetMaxHeight() - 1;
		while (true)
		{
			Node* next = x->Next(level);
			if (next == nullptr || compare_(next->key, key) >= 0)
			{
				if (level == 0)
					return x;
				else
					level--;
			}
			else
			{
				x = next;
			}
		}
	}

	template <typename Key, class Comparator>
	typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::GetLastNode() const
	{
		Node* x = head_;
		int level = GetMaxHeight() - 1;
		while (true)
		{
			Node* next = x->Next(level);
			if (next == nullptr)
			{
				if (level == 0)
					return x; // The node which points to nullptr at this level, is the last node.
				else
					level--;
			}
			else
			{
				x = next;
			}
		}
	}

	template <typename Key, class Comparator>
	bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const
	{
		return n != nullptr && compare_(n->key, key) < 0;
	}

	template <typename Key, class Comparator>
	int SkipList<Key, Comparator>::RandomHeight()
	{
		static const unsigned int kBranching = 4;
		int height = 1;
		while (height < kMaxHeight && rnd_.OneIn(kBranching))
		{
			height++;
		}
		assert(height > 0);
		assert(height <= kMaxHeight);
		return height;
	}
}

#endif