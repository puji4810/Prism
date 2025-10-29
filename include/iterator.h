#ifndef ITERATOR_H
#define ITERATOR_H

#include "slice.h"
#include "status.h"
#include <memory>

namespace prism
{
	class Iterator
	{
	public:
		Iterator();

		Iterator(const Iterator&) = delete;
		Iterator& operator=(const Iterator&) = delete;
		Iterator(Iterator&&) = delete;
		Iterator& operator=(Iterator&&) = delete;
		virtual ~Iterator();

		virtual bool Valid() const = 0;
		virtual void SeekToFirst() = 0;
		virtual void SeekToLast() = 0;
		virtual void Seek(const Slice& target) = 0;
		virtual void Next() = 0;
		virtual void Prev() = 0;
		virtual Slice key() const = 0;
		virtual Slice value() const = 0;
		virtual Status status() const = 0;

		using CleanupFunction = void (*)(void* arg1, void* arg2);
		void RegisterCleanup(CleanupFunction function, void* arg1, void* arg2);

	private:
		struct CleanupNode
		{
			bool IsEmpty() const { return function == nullptr; }
			void Run()
			{
				assert(function != nullptr);
				(*function)(arg1, arg2);
			}
			CleanupFunction function;
			void* arg1;
			void* arg2;
			CleanupNode* next;
		};

		// use CleanupNode can avoid heap allocation for the first node.
		CleanupNode cleanup_head_;
	};

	// Return an empty iterator (yields nothing).
	Iterator* NewEmptyIterator();

	std::unique_ptr<Iterator> NewUniqueEmptyIterator();

	// Return an empty iterator with the specified status.
	Iterator* NewErrorIterator(const Status& status);
}

#endif