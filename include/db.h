#ifndef PRISM_DB_H
#define PRISM_DB_H

#include <string>
#include <memory>
#include "status.h"
#include "slice.h"
#include "write_batch.h"

namespace prism
{
	class DB
	{
	public:
		DB() = default;
		virtual ~DB();
		static std::unique_ptr<DB> Open(const std::string& dbname);
		virtual Status Put(const Slice& key, const Slice& value) = 0;
		virtual Result<std::string> Get(const Slice& key) = 0;
		virtual Status Delete(const Slice& key) = 0;
		virtual Status Write(WriteBatch& batch) = 0;
	};
} // namespace prism

#endif // PRISM_DB_H