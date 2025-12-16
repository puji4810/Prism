#include "filename.h"
#include "env.h"
#include "logging.h"
#include <cassert>
#include <cstdio>
#include <cstring>

namespace prism
{

	// A utility routine: write "data" to the named file and Sync() it.
	Status WriteStringToFileSync(Env* env, const Slice& data, const std::string& fname);

	namespace
	{
		std::string MakeFileName(const std::string& dbname, uint64_t number, const char* suffix)
		{
			char buf[100];
			std::snprintf(buf, sizeof(buf), "/%06llu.%s", static_cast<unsigned long long>(number), suffix);
			return dbname + buf;
		}
	} // namespace

	std::string LogFileName(const std::string& dbname, uint64_t number)
	{
		assert(number > 0);
		return MakeFileName(dbname, number, "log");
	}

	std::string TableFileName(const std::string& dbname, uint64_t number)
	{
		assert(number > 0);
		return MakeFileName(dbname, number, "ldb");
	}

	std::string SSTTableFileName(const std::string& dbname, uint64_t number)
	{
		assert(number > 0);
		return MakeFileName(dbname, number, "sst");
	}

	std::string DescriptorFileName(const std::string& dbname, uint64_t number)
	{
		assert(number > 0);
		char buf[100];
		std::snprintf(buf, sizeof(buf), "/MANIFEST-%06llu", static_cast<unsigned long long>(number));
		return dbname + buf;
	}

	std::string CurrentFileName(const std::string& dbname) { return dbname + "/CURRENT"; }

	std::string LockFileName(const std::string& dbname) { return dbname + "/LOCK"; }

	std::string TempFileName(const std::string& dbname, uint64_t number)
	{
		assert(number > 0);
		return MakeFileName(dbname, number, "dbtmp");
	}

	std::string InfoLogFileName(const std::string& dbname) { return dbname + "/LOG"; }

	std::string OldInfoLogFileName(const std::string& dbname) { return dbname + "/LOG.old"; }

	// Owned filenames have the form:
	//   dbname/CURRENT
	//   dbname/LOCK
	//   dbname/LOG
	//   dbname/LOG.old
	//   dbname/MANIFEST-[0-9]+
	//   dbname/[0-9]+.(log|sst|ldb)
	bool ParseFileName(const std::string& filename, uint64_t* number, FileType* type)
	{
		Slice rest(filename);
		if (rest == "CURRENT")
		{
			*number = 0;
			*type = FileType::kCurrentFile;
		}
		else if (rest == "LOCK")
		{
			*number = 0;
			*type = FileType::kDBLockFile;
		}
		else if (rest == "LOG" || rest == "LOG.old")
		{
			*number = 0;
			*type = FileType::kInfoLogFile;
		}
		else if (rest.starts_with("MANIFEST-"))
		{
			rest.remove_prefix(std::strlen("MANIFEST-"));
			uint64_t num;
			if (!ConsumeDecimalNumber(&rest, &num))
			{
				return false;
			}
			if (!rest.empty())
			{
				return false;
			}
			*type = FileType::kDescriptorFile;
			*number = num;
		}
		else
		{
			uint64_t num;
			if (!ConsumeDecimalNumber(&rest, &num))
			{
				return false;
			}
			Slice suffix = rest;
			if (suffix == Slice(".log"))
			{
				*type = FileType::kLogFile;
			}
			else if (suffix == Slice(".sst") || suffix == Slice(".ldb"))
			{
				*type = FileType::kTableFile;
			}
			else if (suffix == Slice(".dbtmp"))
			{
				*type = FileType::kTempFile;
			}
			else
			{
				return false;
			}
			*number = num;
		}
		return true;
	}

	Status SetCurrentFile(Env* env, const std::string& dbname, uint64_t descriptor_number)
	{
		std::string manifest = DescriptorFileName(dbname, descriptor_number);
		Slice contents = manifest;
		assert(contents.starts_with(dbname + "/"));
		contents.remove_prefix(dbname.size() + 1);
		std::string tmp = TempFileName(dbname, descriptor_number);
		Status s = WriteStringToFileSync(env, Slice(contents.ToString() + "\n"), tmp);
		if (s.ok())
		{
			s = env->RenameFile(tmp, CurrentFileName(dbname));
		}
		if (!s.ok())
		{
			env->RemoveFile(tmp);
		}
		return s;
	}
} // namespace prism
