#include "env.h"
#include "slice.h"
#include "status.h"
#include "port/port_config.h"
#include "port/thread_annotations.h"
#include "posix_logger.h"
#include <expected>
#include <memory>
#include <set>
#include <cassert>
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <condition_variable>
#include <fcntl.h>
#include <dirent.h>
#include <limits>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <type_traits>
#include <thread>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <atomic>
#include <utility>

namespace prism
{
	constexpr const int kOpenBaseFlags = HAVE_O_CLOEXEC ? O_CLOEXEC : 0;
	constexpr const size_t kWritableFileBufferSize = 65536;

	Status PosixError(const std::string& context, int error_number)
	{
		if (error_number == ENOENT)
		{
			return Status::NotFound(context, std::strerror(error_number));
		}
		else
		{
			return Status::IOError(context, std::strerror(error_number));
		}
	}

	class Limiter
	{
	public:
		Limiter(int max_acquires)
		    :
#if !defined(NDEBUG)
		    max_acquires_(max_acquires)
		    ,
#endif // !defined(NDEBUG)
		    acquires_allowed_(max_acquires)
		{
			assert(max_acquires >= 0);
		}

		Limiter(const Limiter&) = delete;
		Limiter operator=(const Limiter&) = delete;
		Limiter(const Limiter&&) = delete;
		Limiter& operator=(const Limiter&&) = delete;

		bool Acquire()
		{
			int old_acquires_allowed = acquires_allowed_.fetch_sub(1, std::memory_order_relaxed);
			if (old_acquires_allowed > 0)
			{
				return true;
			}

			int pre_increment_acquires_allowed = acquires_allowed_.fetch_add(1, std::memory_order_relaxed);

#if !defined(NDEBUG)
			assert(pre_increment_acquires_allowed < max_acquires_);
#endif

			return false;
		}

		void Release()
		{
			int old_acquires_allowed = acquires_allowed_.fetch_add(1, std::memory_order_relaxed);
#if !defined(NDEBUG)
			assert(old_acquires_allowed < max_acquires_);
#endif
		}

	private:
#if !defined(NDEBUG)
		const int max_acquires_;
#endif
		std::atomic<int> acquires_allowed_;
	};

	class PosixSequentialFile final: public SequentialFile
	{
	public:
		PosixSequentialFile(std::string filename, int fd)
		    : fd_(fd)
		    , filename_(std::move(filename))
		{
		}
		~PosixSequentialFile() override { close(fd_); }

		Status Read(size_t n, Slice* result, char* scratch) override
		{
			Status status;
			while (true)
			{
				::ssize_t read_size = ::read(fd_, scratch, n);
				if (read_size < 0)
				{ // Read error.
					if (errno == EINTR)
					{
						continue; // Retry
					}
					status = PosixError(filename_, errno);
					break;
				}
				*result = Slice(scratch, read_size);
				break;
			}
			return status;
		}

		Status Skip(uint64_t n) override
		{
			if (::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1))
			{
				return PosixError(filename_, errno);
			}
			return Status::OK();
		}

	private:
		const int fd_;
		const std::string filename_;
	};

	class PosixRandomAccessFile: public RandomAccessFile
	{
		std::string filename_;
		int fd_;
		Limiter* fd_limiter_;

		// A file descriptor that is permanent and will not be closed until the file is destroyed.
		// If the file descriptor is not permanent, it will be closed and a new file descriptor will be opened on every read.
		const bool has_permanent_fd_;

	public:
		PosixRandomAccessFile(std::string filename, int fd, Limiter* fd_limiter)
		    : has_permanent_fd_(fd_limiter->Acquire())
		    , fd_limiter_(fd_limiter)
		    , filename_(std::move(filename))
		{
			fd_ = has_permanent_fd_ ? fd : -1;
			if (!has_permanent_fd_)
			{
				assert(fd_ == -1); // fd = -1, if has_permanent_fd_ is false.
				::close(fd); // quick close
			}
		}

		~PosixRandomAccessFile() override
		{
			if (has_permanent_fd_)
			{
				assert(fd_ != -1);
				::close(fd_);
				fd_limiter_->Release();
			}
		}

		Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override
		{
			int fd = fd_;
			if (!has_permanent_fd_)
			{
				fd = ::open(filename_.c_str(), O_RDONLY | kOpenBaseFlags);
				if (fd < 0)
				{
					return Status::IOError(filename_, strerror(errno));
				}
			}

			assert(fd != -1);
			Status status;
			ssize_t r = ::pread(fd, scratch, n, static_cast<off_t>(offset));
			*result = Slice(scratch, (r < 0) ? 0 : r);
			if (r < 0)
			{
				status = Status::IOError(filename_, strerror(errno));
			}
			if (!has_permanent_fd_)
			{
				assert(fd != fd_);
				::close(fd);
			}
			return status;
		}
	};

	// A class to manage the mmap region
	// mmap_base_[0, length-1] points to the region mmaped from the file
	// It's the successful res from the call to mmap(). This class takes the ownership
	//
	// Note:
	//
	// |mmap_limiter| must outlive this instance. The caller must have already
	// acquired the right to use one mmap region, which will be released when this
	// instance is destroyed.(As leveldb not allow exception, we just follow this)
	class PosixMmapReadableFile: public RandomAccessFile
	{
	public:
		PosixMmapReadableFile(std::string filename, char* mmap_base, size_t length, Limiter* mmap_limiter)
		    : mmap_base_(mmap_base)
		    , length_(length)
		    , mmap_limiter_(mmap_limiter)
		    , filename_(std::move(filename))
		{
		}

		~PosixMmapReadableFile() override
		{
			::munmap(mmap_base_, length_);
			mmap_limiter_->Release();
		}

		Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override
		{
			(void)scratch;
			const uint64_t length = static_cast<uint64_t>(length_);
			if (offset > length || static_cast<uint64_t>(n) > length - offset)
			{
				*result = Slice();
				return PosixError(filename_, EINVAL);
			}

			*result = Slice(mmap_base_ + offset, n);
			return Status::OK();
		}

	private:
		char* const mmap_base_;
		const size_t length_;
		Limiter* const mmap_limiter_;
		const std::string filename_;
	};

	class PosixWritableFile final: public WritableFile
	{
	public:
		PosixWritableFile(std::string fname, int fd)
		    : pos_(0)
		    , fd_(fd)
		    , is_manifest_(IsManifest(fname))
		    , filename_(std::move(fname))
		    , dirname_(Dirname(filename_))
		{
			assert(fd_ >= 0);
		}

		~PosixWritableFile() override
		{
			if (fd_ >= 0)
			{
				Close();
			}
		}

		Status Append(const Slice& data) override
		{
			size_t write_size = data.size();
			const char* write_data = data.data();

			size_t copy_size = std::min(write_size, kWritableFileBufferSize - pos_);
			std::memcpy(buf_ + pos_, write_data, copy_size);
			write_data += copy_size;
			write_size -= copy_size;
			pos_ += copy_size;
			if (write_size == 0)
			{
				return Status::OK();
			}

			// Cant fit in the buffer, we need at least one flush
			// compare the rest size
			// small writes to the buf
			// large writes to the disk
			Status status = FlushBuffer();
			if (!status.ok())
			{
				return status;
			}

			if (write_size < kWritableFileBufferSize)
			{
				std::memcpy(buf_, write_data, write_size);
				pos_ = write_size;
				return Status::OK();
			}

			return WriteUnbuffered(write_data, write_size);
		}

		Status Close() override
		{
			Status status = FlushBuffer();
			const auto close_res = ::close(fd_);
			if (close_res < 0 && status.ok())
			{
				status = PosixError(filename_, errno);
			}
			fd_ = -1;
			return status;
		}

		Status Flush() override { return FlushBuffer(); }

		Status Sync() override
		{
			// Ensure every new file is exit before the manifest file flushed to the disk
			Status status = SyncDirIfManifest();
			if (!status.ok())
			{
				return status;
			}

			status = FlushBuffer();
			if (!status.ok())
			{
				return status;
			}

			return SyncFd(fd_, filename_);
		}

	private:
		Status FlushBuffer()
		{
			Status status = WriteUnbuffered(buf_, pos_);
			pos_ = 0;
			return status;
		}

		Status WriteUnbuffered(const char* data, size_t size)
		{
			while (size > 0)
			{
				ssize_t write_res = ::write(fd_, data, size);
				if (write_res < 0)
				{
					if (errno == EINTR)
					{
						continue; // retry
					}
					return PosixError(filename_, errno);
				}

				data += write_res;
				size -= static_cast<size_t>(write_res);
			}
			return Status::OK();
		}

		static std::string Dirname(const std::string& filename)
		{
			std::string::size_type separator_pos = filename.rfind('/');
			if (separator_pos == std::string::npos)
			{
				return std::string(".");
			}
			// The filename component should not contain a path separator. If it does,
			// the splitting was done incorrectly.
			assert(filename.find('/', separator_pos + 1) == std::string::npos);

			return filename.substr(0, separator_pos); // [0, separator_pos)
		}

		// Extracts the file name from a path pointing to a file.
		//
		// The returned Slice points to |filename|'s data buffer, so it is only valid
		// while |filename| is alive and unchanged.
		static Slice Basename(const std::string& filename)
		{
			std::string::size_type separator_pos = filename.rfind('/');
			if (separator_pos == std::string::npos)
			{
				return Slice(filename);
			}
			// The filename component should not contain a path separator. If it does,
			// the splitting was done incorrectly.
			assert(filename.find('/', separator_pos + 1) == std::string::npos);

			// dir1/dir2/dir3/fname
			//               ^
			//               | pos
			return Slice(filename.data() + separator_pos + 1, filename.length() - separator_pos - 1);
		}

		Status SyncDirIfManifest()
		{
			Status status;
			if (!is_manifest_)
			{
				return status;
			}

			int fd = ::open(dirname_.c_str(), O_RDONLY | kOpenBaseFlags);

			if (fd < 0)
			{
				status = PosixError(dirname_, errno);
			}
			else
			{
				status = SyncFd(fd, dirname_);
				::close(fd);
			}
			return status;
		}

		static Status SyncFd(int fd, const std::string& fd_path)
		{
#if HAVE_FULLFSYNC
			if (::fcntl(fd, F_FULLFSYNC) == 0)
			{
				return Status::OK();
			}
#endif

#if HAVE_FDATASYNC
			bool sync_success = (::fdatasync(fd) == 0);
#else
			bool sync_success = (::fsync(fd) == 0);
#endif

			if (sync_success)
			{
				return Status::OK();
			}
			return PosixError(fd_path, errno);
		}

		// True if the given file is a manifest file.
		static bool IsManifest(const std::string& filename) { return Basename(filename).starts_with("MANIFEST"); }

		// buf_[0, pos_ - 1] contains data to be written to fd_.
		char buf_[kWritableFileBufferSize];
		size_t pos_;
		int fd_;

		const bool is_manifest_; // True if the file's name starts with MANIFEST.
		const std::string filename_;
		const std::string dirname_; // The directory of filename_.
	};

	// Lock Or Unlock for A file
	int LockOrUnlock(int fd, bool lock)
	{
		errno = 0;
		::flock file_lock_info;
		std::memset(&file_lock_info, 0, sizeof(file_lock_info)); // clear
		file_lock_info.l_type = (lock ? F_WRLCK : F_UNLCK); // write lock for true, And unlock for false
		file_lock_info.l_whence = SEEK_SET;
		file_lock_info.l_start = 0;
		file_lock_info.l_len = 0;
		return fcntl(fd, F_SETLK, &file_lock_info);
	}

	class PosixFileLock: public FileLock
	{
	public:
		PosixFileLock(int fd, std::string fname)
		    : fd_(fd)
		    , filename_(std::move(fname))
		{
		}

		int fd() const { return fd_; };
		const std::string& filename() { return filename_; }

	private:
		const int fd_;
		const std::string filename_;
	};

	class PosixLockTable
	{
	public:
		bool Insert(const std::string& fname) LOCKS_EXCLUDED(mu_)
		{
			mu_.lock();
			bool succeeded = locked_files_.insert(fname).second;
			mu_.unlock();
			return succeeded;
		}

		void Remove(const std::string& fname) LOCKS_EXCLUDED(mu_)
		{
			std::lock_guard<std::mutex> guard(mu_);
			locked_files_.erase(fname);
		}

	private:
		std::mutex mu_;
		std::set<std::string> locked_files_ GUARDED_BY(mu_);
	};

	class PosixEnv: public Env
	{
	public:
		PosixEnv();
		~PosixEnv() override
		{
			static const char msg[] = "PosixEnv singleton destroyed. Unsupported behavior!\n";
			std::fwrite(msg, 1, sizeof(msg), stderr);
			std::abort();
		}

		Result<std::unique_ptr<RandomAccessFile>> NewRandomAccessFile(const std::string& fname) override
		{
			int fd = ::open(fname.c_str(), O_RDONLY | kOpenBaseFlags);
			if (fd < 0)
			{
				return std::unexpected(Status::IOError(fname, strerror(errno)));
			}

			if (!mmap_limiter_.Acquire())
			{
				return std::make_unique<PosixRandomAccessFile>(fname, fd, &fd_limiter_);
			}

			uint64_t file_size;
			Status status = GetFileSize(fname, &file_size);
			if (status.ok())
			{
				void* mmap_base = ::mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
				if (mmap_base != MAP_FAILED)
				{
					return std::make_unique<PosixMmapReadableFile>(fname, reinterpret_cast<char*>(mmap_base), file_size, &mmap_limiter_);
				}
				else
				{
					status = PosixError(fname, errno);
				}
			}

			::close(fd);
			if (!status.ok())
			{
				mmap_limiter_.Release();
				return std::unexpected(status);
			}

			return std::unexpected(Status::Corruption("should not reach here"));
		}

		Result<std::unique_ptr<SequentialFile>> NewSequentialFile(const std::string& filename) override
		{
			int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
			if (fd < 0)
			{
				return std::unexpected<Status>(PosixError(filename, errno));
			}
			return std::make_unique<PosixSequentialFile>(filename, fd);
		}

		Result<std::unique_ptr<WritableFile>> NewWritableFile(const std::string& fname) override
		{
			int fd = ::open(fname.c_str(), O_WRONLY | O_CREAT | O_TRUNC | kOpenBaseFlags, 0644);
			if (fd < 0)
			{
				return std::unexpected<Status>(PosixError(fname, errno));
			}
			return std::make_unique<PosixWritableFile>(fname, fd);
		}

		Status NewAppendableFile(const std::string& fname, WritableFile** result) override
		{
			int fd = ::open(fname.c_str(), O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
			if (fd < 0)
			{
				*result = nullptr;
				return PosixError(fname, errno);
			}

			*result = new PosixWritableFile(fname, fd);
			return Status::OK();
		}

		bool FileExists(const std::string& fname) override { return (::access(fname.c_str(), F_OK) == 0); }

		Status GetChildren(const std::string& dir_path, std::vector<std::string>* result) override
		{
			result->clear();
			::DIR* dir = ::opendir(dir_path.c_str());
			if (dir == nullptr)
			{
				return PosixError(dir_path, errno);
			}
			::dirent* entry;
			while ((entry = ::readdir(dir)) != nullptr)
			{
				result->emplace_back(entry->d_name);
			}
			::closedir(dir);
			return Status::OK();
		}

		Status RemoveFile(const std::string& filename) override
		{
			if (::unlink(filename.c_str()) != 0)
			{
				return PosixError(filename, errno);
			}
			return Status::OK();
		}

		Status CreateDir(const std::string& dirname) override
		{
			if (::mkdir(dirname.c_str(), 0755) != 0)
			{
				if (errno == EEXIST)
				{
					struct ::stat statbuf;
					if (::stat(dirname.c_str(), &statbuf) == 0 && S_ISDIR(statbuf.st_mode))
					{
						return Status::OK();
					}
				}
				return PosixError(dirname, errno);
			}
			return Status::OK();
		}

		Status RemoveDir(const std::string& dirname) override
		{
			if (::rmdir(dirname.c_str()) != 0)
			{
				return PosixError(dirname, errno);
			}
			return Status::OK();
		}

		Status GetFileSize(const std::string& fname, uint64_t* size) override
		{
			struct ::stat file_stat;
			if (::stat(fname.c_str(), &file_stat) != 0)
			{
				*size = 0;
				return PosixError(fname, errno);
			}
			*size = static_cast<uint64_t>(file_stat.st_size);
			return Status::OK();
		}

		Status RenameFile(const std::string& from, const std::string& to) override
		{
			if (std::rename(from.c_str(), to.c_str()) != 0)
			{
				return PosixError(from, errno);
			}
			return Status::OK();
		}

		Status LockFile(const std::string& fname, FileLock** lock) override
		{
			*lock = nullptr;

			int fd = ::open(fname.c_str(), O_RDWR | O_CREAT | kOpenBaseFlags, 0644);
			if (fd < 0)
			{
				return PosixError(fname, errno);
			}

			if (!locks_.Insert(fname))
			{
				::close(fd);
				return Status::IOError("lock " + fname, "already held by process");
			}

			if (LockOrUnlock(fd, true) == -1)
			{
				int lock_errno = errno;
				::close(fd);
				locks_.Remove(fname);
				return PosixError("lock " + fname, lock_errno);
			}

			*lock = new PosixFileLock(fd, fname);
			return Status::OK();
		}

		Status UnlockFile(FileLock* lock) override
		{
			PosixFileLock* posix_file_lock = static_cast<PosixFileLock*>(lock);
			if (LockOrUnlock(posix_file_lock->fd(), false) == -1)
			{
				return PosixError("unlock " + posix_file_lock->filename(), errno);
			}
			locks_.Remove(posix_file_lock->filename());
			::close(posix_file_lock->fd());
			delete posix_file_lock;
			return Status::OK();
		}

		void Schedule(void (*background_work_function)(void* background_work_arg), void* background_work_arg) override;

		void StartThread(void (*thread_main)(void* thread_main_arg), void* thread_main_arg) override
		{
			std::thread new_thread(thread_main, thread_main_arg);
			new_thread.detach();
		}

		Status GetTestDirectory(std::string* result) override
		{
			const char* env = std::getenv("TEST_TMPDIR");
			if (env && env[0] != '\0')
			{
				*result = env;
			}
			else
			{
				char buf[100];
				std::snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d", static_cast<int>(::geteuid()));
				*result = buf;
			}

			// The CreateDir status is ignored because the directory may already exist.
			CreateDir(*result);

			return Status::OK();
		}

		Status NewLogger(const std::string& fname, Logger** result) override
		{
			int fd = ::open(fname.c_str(), O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
			if (fd < 0)
			{
				*result = nullptr;
				return PosixError(fname, errno);
			}

			std::FILE* fp = ::fdopen(fd, "w");
			if (fp == nullptr)
			{
				::close(fd);
				*result = nullptr;
				return PosixError(fname, errno);
			}

			*result = new PosixLogger(fp);
			return Status::OK();
		}

		uint64_t NowMicros() override
		{
			static constexpr uint64_t kUsecondsPerSecond = 1000000;
			struct ::timeval tv;
			::gettimeofday(&tv, nullptr);
			return static_cast<uint64_t>(tv.tv_sec) * kUsecondsPerSecond + static_cast<uint64_t>(tv.tv_usec);
		}

		void SleepForMicroseconds(int micros) override { std::this_thread::sleep_for(std::chrono::microseconds(micros)); }

	private:
		void BackgroundThreadMain();

		static void BackgroundThreadEntryPoint(PosixEnv* env) { env->BackgroundThreadMain(); }

		struct BackgroundWorkItem
		{
			explicit BackgroundWorkItem(void (*function)(void* arg), void* arg)
			    : function(function)
			    , arg(arg)
			{
			}

			void (*function)(void*);
			void* arg;
		};

		std::mutex background_work_mutex_;
		std::condition_variable background_work_cv_;
		bool started_background_thread_ GUARDED_BY(background_work_mutex_) = false;
		std::queue<BackgroundWorkItem> background_work_queue_ GUARDED_BY(background_work_mutex_);

		Limiter fd_limiter_;
		Limiter mmap_limiter_;

		PosixLockTable locks_;
	};

	namespace
	{
		constexpr const int kDefaultMmapLimit = (sizeof(void*) >= 8) ? 1000 : 0;

		int MaxMmaps() { return kDefaultMmapLimit; }

		int MaxOpenFiles()
		{
			struct ::rlimit rlim;
			if (::getrlimit(RLIMIT_NOFILE, &rlim) != 0)
			{
				return 50;
			}
			if (rlim.rlim_cur == RLIM_INFINITY)
			{
				return std::numeric_limits<int>::max();
			}
			return static_cast<int>(rlim.rlim_cur / 5);
		}
	}

	PosixEnv::PosixEnv()
	    : fd_limiter_(MaxOpenFiles())
	    , mmap_limiter_(MaxMmaps())
	{
	}

	void PosixEnv::Schedule(void (*background_work_function)(void* background_work_arg), void* background_work_arg)
	{
		std::unique_lock<std::mutex> lock(background_work_mutex_);

		if (!started_background_thread_)
		{
			started_background_thread_ = true;
			std::thread background_thread(BackgroundThreadEntryPoint, this);
			background_thread.detach();
		}

		const bool was_empty = background_work_queue_.empty();
		background_work_queue_.emplace(background_work_function, background_work_arg);
		if (was_empty)
		{
			background_work_cv_.notify_one();
		}
	}

	void PosixEnv::BackgroundThreadMain()
	{
		while (true)
		{
			std::unique_lock<std::mutex> lock(background_work_mutex_);
			while (background_work_queue_.empty())
			{
				background_work_cv_.wait(lock);
			}

			assert(!background_work_queue_.empty());
			auto background_work_function = background_work_queue_.front().function;
			void* background_work_arg = background_work_queue_.front().arg;
			background_work_queue_.pop();

			lock.unlock();
			background_work_function(background_work_arg);
		}
	}

	Env* Env::Default()
	{
		static PosixEnv* default_env = new PosixEnv();
		return default_env;
	}
}
