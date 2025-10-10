#ifndef PRISM_DB_IMPL_H
#define PRISM_DB_IMPL_H

#include "db.h"
#include "log_writer.h"
#include "log_reader.h"
#include <unordered_map>

namespace prism
{
	class DBImpl: public DB
	{
	public:
		DBImpl(const std::string& dbname);
		~DBImpl() override;
		Status Put(const Slice& key, const Slice& value) override;
		Result<std::string> Get(const Slice& key) override;
		Status Delete(const Slice& key) override;
		Status Write(WriteBatch& batch) override;

	private:
		class RecoveryHandler;

		Status ApplyBatch(WriteBatch& batch);

		std::unordered_map<std::string, std::string> store_;
		log::Writer writer_;
		log::Reader reader_;
		uint64_t sequence_;
	};
} // namespace prism

#endif // PRISM_DB_IMPL_H