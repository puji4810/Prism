// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "env.h"
#include <cstdarg>

namespace prism
{

	// Default implementations for deprecated methods
	Result<std::unique_ptr<WritableFile>> Env::NewAppendableFile(const std::string& fname)
	{
		return std::unexpected<Status>(Status::NotSupported("NewAppendableFile not supported", fname));
	}

	// Legacy method forwarding for compatibility
	Status Env::RemoveDir(const std::string& dirname) { return DeleteDir(dirname); }

	Status Env::DeleteDir(const std::string& dirname) { return RemoveDir(dirname); }

	Status Env::RemoveFile(const std::string& fname) { return DeleteFile(fname); }

	Status Env::DeleteFile(const std::string& fname) { return RemoveFile(fname); }

	// Utility: Log with variable arguments
	void Log(Logger* info_log, const char* format, ...)
	{
		if (info_log != nullptr)
		{
			std::va_list ap;
			va_start(ap, format);
			info_log->Logv(format, ap);
			va_end(ap);
		}
	}

	// Internal helper for WriteStringToFile
	static Status DoWriteStringToFile(Env* env, const Slice& data, const std::string& fname, bool should_sync)
	{
		Status s = Status::OK();
		Result<std::unique_ptr<WritableFile>> result = env->NewWritableFile(fname);
		if (!result.has_value())
		{
			return result.error();
		}
		auto file = std::move(result.value());
		s = file->Append(data);
		if (s.ok() && should_sync)
		{
			s = file->Sync();
		}
		if (s.ok())
		{
			s = file->Close();
		}
		if (!s.ok())
		{
			env->RemoveFile(fname);
		}
		return s;
	}

	// Utility: Write string to file (without sync)
	Status WriteStringToFile(Env* env, const Slice& data, const std::string& fname) { return DoWriteStringToFile(env, data, fname, false); }

	// Utility: Write string to file (with sync)
	Status WriteStringToFileSync(Env* env, const Slice& data, const std::string& fname)
	{
		return DoWriteStringToFile(env, data, fname, true);
	}

	// Utility: Read entire file to string
	Status ReadFileToString(Env* env, const std::string& fname, std::string* data)
	{
		data->clear();
		Status s;
		auto file = env->NewSequentialFile(fname);
		if (!file)
		{
			return file.error();
		}
		static constexpr int kBufferSize = 8192;
		char* space = new char[kBufferSize];
		while (true)
		{
			Slice fragment;
			s = (*file)->Read(kBufferSize, &fragment, space);
			if (!s.ok())
			{
				break;
			}
			data->append(fragment.data(), fragment.size());
			if (fragment.empty())
			{
				break;
			}
		}
		delete[] space;
		return s;
	}

} // namespace prism
