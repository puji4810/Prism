#ifndef PRISM_DB_H
#define PRISM_DB_H

#include <optional>
#include <string>
#include <memory>

namespace prism
{
	class DB
	{
	public:
		DB() = default;
		virtual ~DB();
		static std::unique_ptr<DB> Open(const std::string& dbname);
		virtual void Put(const std::string& key, const std::string& value) = 0;
		virtual std::optional<std::string> Get(const std::string& key) = 0;
		virtual void Delete(const std::string& key) = 0;
	};
} // namespace prism

#endif // PRISM_DB_H