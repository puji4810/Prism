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
		void Put(const std::string& key, const std::string& value) override;
		std::optional<std::string> Get(const std::string& key) override;
		void Delete(const std::string& key) override;

	private:
		std::unordered_map<std::string, std::string> store_;
		log::Writer writer_;
		log::Reader reader_;
	};
} // namespace prism

#endif // PRISM_DB_IMPL_H