#ifndef WRITE_BATCH_H_
#define WRITE_BATCH_H_

#include <string>
#include "slice.h"
#include "status.h"

namespace prism
{
	static const size_t kHeader = 12;
	class WriteBatch
	{
	public:
		class Handler
		{
		public:
			virtual ~Handler() = default;
			virtual void Put(const Slice& key, const Slice& value) = 0;
			virtual void Delete(const Slice& key) = 0;
		};

		WriteBatch();
		~WriteBatch() = default;
		WriteBatch(const WriteBatch&) = default;
		WriteBatch& operator=(const WriteBatch&) = default;
		WriteBatch(WriteBatch&&) = default;
		WriteBatch& operator=(WriteBatch&&) = default;

		// Store the mapping "key->value" in the database.
		void Put(const Slice& key, const Slice& value);

		// If the database contains a mapping for "key", erase it.  Else do nothing.
		void Delete(const Slice& key);

		Status Iterate(Handler* handler) const;

		void Append(const WriteBatch& source);

		// Clear all updates buffered in this batch.
		void Clear();

		size_t ApproximateSize() const;

	private:
		friend class WriteBatchInternal;
		std::string rep_;
	};
} // namespace prism

#endif // WRITE_BATCH_H_