#include "env.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>

namespace prism
{

	class PosixRandomAccessFile: public RandomAccessFile
	{
		std::string filename_;
		int fd_;

	public:
		PosixRandomAccessFile(const std::string& fname, int fd)
		    : filename_(fname)
		    , fd_(fd)
		{
		}

		~PosixRandomAccessFile() override { close(fd_); }

		Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override
		{
			ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset));
			*result = Slice(scratch, (r < 0) ? 0 : r);
			if (r < 0)
			{
				return Status::IOError(filename_, strerror(errno));
			}
			return Status::OK();
		}
	};

	class PosixEnv: public Env
	{
	public:
		Status NewRandomAccessFile(const std::string& fname, RandomAccessFile** result) override
		{
			int fd = open(fname.c_str(), O_RDONLY);
			if (fd < 0)
			{
				*result = nullptr;
				return Status::IOError(fname, strerror(errno));
			}
			*result = new PosixRandomAccessFile(fname, fd);
			return Status::OK();
		}

		Status NewSequentialFile(const std::string&, SequentialFile**) override { return Status::NotSupported("Not implemented yet"); }

		class PosixWritableFile: public WritableFile
		{
		public:
			PosixWritableFile(const std::string& fname, int fd)
			    : filename_(fname)
			    , fd_(fd)
			{
			}

			~PosixWritableFile() override
			{
				if (fd_ >= 0)
				{
					::close(fd_);
				}
			}

			Status Append(const Slice& data) override
			{
				const char* p = data.data();
				size_t n = data.size();
				while (n > 0)
				{
					ssize_t r = ::write(fd_, p, n);
					if (r < 0)
					{
						if (errno == EINTR)
						{
							continue;
						}
						return Status::IOError(filename_, std::strerror(errno));
					}
					p += r;
					n -= static_cast<size_t>(r);
				}
				return Status::OK();
			}

			Status Close() override
			{
				if (fd_ >= 0)
				{
					if (::close(fd_) != 0)
					{
						fd_ = -1;
						return Status::IOError(filename_, std::strerror(errno));
					}
					fd_ = -1;
				}
				return Status::OK();
			}

			Status Flush() override
			{
				// Rely on OS buffering; no extra flush beyond Sync.
				return Status::OK();
			}

			Status Sync() override
			{
				if (fd_ >= 0)
				{
					if (::fsync(fd_) != 0)
					{
						return Status::IOError(filename_, std::strerror(errno));
					}
				}
				return Status::OK();
			}

		private:
			std::string filename_;
			int fd_{};
		};

		Status NewWritableFile(const std::string& fname, WritableFile** result) override
		{
			int fd = ::open(fname.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
			if (fd < 0)
			{
				*result = nullptr;
				return Status::IOError(fname, std::strerror(errno));
			}
			*result = new PosixWritableFile(fname, fd);
			return Status::OK();
		}
		bool FileExists(const std::string&) override { return false; }
		Status GetChildren(const std::string&, std::vector<std::string>*) override { return Status::NotSupported("Not implemented yet"); }
		Status CreateDir(const std::string&) override { return Status::NotSupported("Not implemented yet"); }
		Status GetFileSize(const std::string&, uint64_t*) override { return Status::NotSupported("Not implemented yet"); }
		Status RenameFile(const std::string&, const std::string&) override { return Status::NotSupported("Not implemented yet"); }
		Status LockFile(const std::string&, FileLock**) override { return Status::NotSupported("Not implemented yet"); }
		Status UnlockFile(FileLock*) override { return Status::NotSupported("Not implemented yet"); }
		void Schedule(void (*)(void*), void*) override {}
		void StartThread(void (*)(void*), void*) override {}
		Status GetTestDirectory(std::string*) override { return Status::NotSupported("Not implemented yet"); }
		Status NewLogger(const std::string&, Logger**) override { return Status::NotSupported("Not implemented yet"); }
		uint64_t NowMicros() override { return 0; }
		void SleepForMicroseconds(int) override {}
	};

	Env* Env::Default()
	{
		static PosixEnv default_env;
		return &default_env;
	}

}
