#include "db_impl.h"
#include "slice.h"
#include <sstream>
#include <memory>

namespace prism
{
	class DBImpl;

	DB::~DB() = default;

	std::unique_ptr<DB> DB::Open(const std::string& dbname)
	{
		return std::make_unique<DBImpl>(dbname);
	}

	DBImpl::DBImpl(const std::string& dbname)
	    : writer_{ dbname }
	    , reader_{ dbname }
	{
		Slice record;
		while (reader_.ReadRecord(record))
		{
			if (record.empty())
				continue;

			std::stringstream ss(record.ToString());
			std::string op, key, value;
			ss >> op >> key;
			
			if (op == "PUT")
			{
				ss >> value;
				store_[key] = value;
			}
			else if (op == "DELETE")
			{
				store_.erase(key);
			}
		}
	}

	DBImpl::~DBImpl() = default;

	void DBImpl::Put(const std::string& key, const std::string& value)
	{
		writer_.AddRecord("PUT " + key + " " + value);
		store_[key] = value;
	}

	std::optional<std::string> DBImpl::Get(const std::string& key)
	{
		auto it = store_.find(key);
		if (it != store_.end())
		{
			return it->second;
		}
		return std::nullopt;
	}

	void DBImpl::Delete(const std::string& key)
	{
		writer_.AddRecord("DELETE " + key);
		store_.erase(key);
	}
}