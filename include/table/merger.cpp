#include "table/merger.h"

#include <cassert>

namespace prism
{
	namespace
	{
		class IteratorWrapper
		{
		public:
			IteratorWrapper()
			    : iter_(nullptr)
			    , valid_(false)
			{
			}

			explicit IteratorWrapper(Iterator* iter)
			    : iter_(nullptr)
			{
				Set(iter);
			}

			~IteratorWrapper() { delete iter_; }

			Iterator* iter() const { return iter_; }

			void Set(Iterator* iter)
			{
				delete iter_;
				iter_ = iter;
				if (iter_ == nullptr)
				{
					valid_ = false;
				}
				else
				{
					Update();
				}
			}

			bool Valid() const { return valid_; }

			Slice key() const
			{
				assert(Valid());
				return key_;
			}

			Slice value() const
			{
				assert(iter_ != nullptr);
				return iter_->value();
			}

			Status status() const
			{
				assert(iter_ != nullptr);
				return iter_->status();
			}

			void Next()
			{
				assert(iter_ != nullptr);
				iter_->Next();
				Update();
			}

			void Prev()
			{
				assert(iter_ != nullptr);
				iter_->Prev();
				Update();
			}

			void Seek(const Slice& k)
			{
				assert(iter_ != nullptr);
				iter_->Seek(k);
				Update();
			}

			void SeekToFirst()
			{
				assert(iter_ != nullptr);
				iter_->SeekToFirst();
				Update();
			}

			void SeekToLast()
			{
				assert(iter_ != nullptr);
				iter_->SeekToLast();
				Update();
			}

		private:
			void Update()
			{
				valid_ = iter_->Valid();
				if (valid_)
				{
					key_ = iter_->key();
				}
			}

			Iterator* iter_;
			bool valid_;
			Slice key_;
		};

		class MergingIterator: public Iterator
		{
		public:
			MergingIterator(const Comparator* comparator, Iterator** children, int n)
			    : comparator_(comparator)
			    , children_(new IteratorWrapper[n])
			    , n_(n)
			    , current_(nullptr)
			    , direction_(Direction::kForward)
			{
				for (int i = 0; i < n_; ++i)
				{
					children_[i].Set(children[i]);
				}
			}

			~MergingIterator() override { delete[] children_; }

			bool Valid() const override { return current_ != nullptr; }

			void SeekToFirst() override
			{
				for (int i = 0; i < n_; ++i)
				{
					children_[i].SeekToFirst();
				}
				FindSmallest();
				direction_ = Direction::kForward;
			}

			void SeekToLast() override
			{
				for (int i = 0; i < n_; ++i)
				{
					children_[i].SeekToLast();
				}
				FindLargest();
				direction_ = Direction::kReverse;
			}

			void Seek(const Slice& target) override
			{
				for (int i = 0; i < n_; ++i)
				{
					children_[i].Seek(target);
				}
				FindSmallest();
				direction_ = Direction::kForward;
			}

			void Next() override
			{
				assert(Valid());

				if (direction_ != Direction::kForward)
				{
					for (int i = 0; i < n_; ++i)
					{
						IteratorWrapper* child = &children_[i];
						if (child != current_)
						{
							child->Seek(key());
							if (child->Valid() && comparator_->Compare(key(), child->key()) == 0)
							{
								child->Next();
							}
						}
					}
					direction_ = Direction::kForward;
				}

				current_->Next();
				FindSmallest();
			}

			void Prev() override
			{
				assert(Valid());

				if (direction_ != Direction::kReverse)
				{
					for (int i = 0; i < n_; ++i)
					{
						IteratorWrapper* child = &children_[i];
						if (child != current_)
						{
							child->Seek(key());
							if (child->Valid())
							{
								child->Prev();
							}
							else
							{
								child->SeekToLast();
							}
						}
					}
					direction_ = Direction::kReverse;
				}

				current_->Prev();
				FindLargest();
			}

			Slice key() const override
			{
				assert(Valid());
				return current_->key();
			}

			Slice value() const override
			{
				assert(Valid());
				return current_->value();
			}

			Status status() const override
			{
				for (int i = 0; i < n_; ++i)
				{
					const Status s = children_[i].status();
					if (!s.ok())
					{
						return s;
					}
				}
				return Status::OK();
			}

		private:
			enum class Direction
			{
				kForward,
				kReverse
			};

			void FindSmallest()
			{
				IteratorWrapper* smallest = nullptr;
				for (int i = 0; i < n_; ++i)
				{
					IteratorWrapper* child = &children_[i];
					if (child->Valid())
					{
						if (smallest == nullptr)
						{
							smallest = child;
						}
						else if (comparator_->Compare(child->key(), smallest->key()) < 0)
						{
							smallest = child;
						}
					}
				}
				current_ = smallest;
			}

			void FindLargest()
			{
				IteratorWrapper* largest = nullptr;
				for (int i = n_ - 1; i >= 0; --i)
				{
					IteratorWrapper* child = &children_[i];
					if (child->Valid())
					{
						if (largest == nullptr)
						{
							largest = child;
						}
						else if (comparator_->Compare(child->key(), largest->key()) > 0)
						{
							largest = child;
						}
					}
				}
				current_ = largest;
			}

			const Comparator* comparator_;
			IteratorWrapper* children_;
			int n_;
			IteratorWrapper* current_;
			Direction direction_;
		};
	}

	Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children, int n)
	{
		assert(n >= 0);
		if (n == 0)
		{
			return NewEmptyIterator();
		}
		if (n == 1)
		{
			return children[0];
		}
		return new MergingIterator(comparator, children, n);
	}
}

