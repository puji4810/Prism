#include "two_level_iterator.h"
#include "options.h"

#include <cassert>

namespace prism
{
	namespace
	{
		// Internal wrapper that caches valid() and key() for an
		// underlying iterator to reduce virtual calls.
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

			// Takes ownership of "iter" and will delete it when
			// destroyed, or when Set() is invoked again.
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

			// Iterator-like interface
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

		class TwoLevelIterator: public Iterator
		{
		public:
			TwoLevelIterator(Iterator* index_iter, BlockFunction block_function, void* arg, const ReadOptions& options)
			    : block_function_(block_function)
			    , arg_(arg)
			    , options_(options)
			    , index_iter_(index_iter)
			    , data_iter_(nullptr)
			{
			}

			~TwoLevelIterator() override = default;

			void Seek(const Slice& target) override
			{
				index_iter_.Seek(target);
				InitDataBlock();
				if (data_iter_.iter() != nullptr)
				{
					data_iter_.Seek(target);
				}
				SkipEmptyDataBlocksForward();
			}

			void SeekToFirst() override
			{
				index_iter_.SeekToFirst();
				InitDataBlock();
				if (data_iter_.iter() != nullptr)
				{
					data_iter_.SeekToFirst();
				}
				SkipEmptyDataBlocksForward();
			}

			void SeekToLast() override
			{
				index_iter_.SeekToLast();
				InitDataBlock();
				if (data_iter_.iter() != nullptr)
				{
					data_iter_.SeekToLast();
				}
				SkipEmptyDataBlocksBackward();
			}

			void Next() override
			{
				assert(Valid());
				data_iter_.Next();
				SkipEmptyDataBlocksForward();
			}

			void Prev() override
			{
				assert(Valid());
				data_iter_.Prev();
				SkipEmptyDataBlocksBackward();
			}

			bool Valid() const override { return data_iter_.Valid(); }

			Slice key() const override
			{
				assert(Valid());
				return data_iter_.key();
			}

			Slice value() const override
			{
				assert(Valid());
				return data_iter_.value();
			}

			Status status() const override
			{
				if (!index_iter_.status().ok())
				{
					return index_iter_.status();
				}
				else if (data_iter_.iter() != nullptr && !data_iter_.status().ok())
				{
					return data_iter_.status();
				}
				else
				{
					return status_;
				}
			}

		private:
			void SaveError(const Status& s)
			{
				if (status_.ok() && !s.ok())
				{
					status_ = s;
				}
			}

			void SkipEmptyDataBlocksForward()
			{
				while (data_iter_.iter() == nullptr || !data_iter_.Valid())
				{
					if (!index_iter_.Valid())
					{
						SetDataIterator(nullptr);
						return;
					}
					index_iter_.Next();
					InitDataBlock();
					if (data_iter_.iter() != nullptr)
					{
						data_iter_.SeekToFirst();
					}
				}
			}

			void SkipEmptyDataBlocksBackward()
			{
				while (data_iter_.iter() == nullptr || !data_iter_.Valid())
				{
					if (!index_iter_.Valid())
					{
						SetDataIterator(nullptr);
						return;
					}
					index_iter_.Prev();
					InitDataBlock();
					if (data_iter_.iter() != nullptr)
					{
						data_iter_.SeekToLast();
					}
				}
			}

			void SetDataIterator(Iterator* data_iter)
			{
				if (data_iter_.iter() != nullptr)
				{
					SaveError(data_iter_.status());
				}
				data_iter_.Set(data_iter);
			}

			void InitDataBlock()
			{
				if (!index_iter_.Valid())
				{
					SetDataIterator(nullptr);
				}
				else
				{
					Slice handle = index_iter_.value();
					if (data_iter_.iter() != nullptr && handle.compare(data_block_handle_) == 0)
					{
						// Reuse existing data iterator for the same block handle.
					}
					else
					{
						Iterator* iter = (*block_function_)(arg_, options_, handle);
						data_block_handle_.assign(handle.data(), handle.size());
						SetDataIterator(iter);
					}
				}
			}

			BlockFunction block_function_;
			void* arg_;
			ReadOptions options_;
			Status status_;
			IteratorWrapper index_iter_;
			IteratorWrapper data_iter_;
			std::string data_block_handle_;
		};
	}

	Iterator* NewTwoLevelIterator(Iterator* index_iter, BlockFunction block_function, void* arg, const ReadOptions& options)
	{
		return new TwoLevelIterator(index_iter, block_function, arg, options);
	}	
}
