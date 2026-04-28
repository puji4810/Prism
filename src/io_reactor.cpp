#include "io_reactor.h"

#include "runtime_executor.h"
#include "runtime_metrics.h"

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <utility>

#ifdef __linux__
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/utsname.h>
#include <unistd.h>
#endif

namespace prism
{
	namespace
	{
		constexpr std::size_t kFallbackIncrement = 1;

#ifdef __linux__
		bool KernelVersionAtLeast(unsigned major, unsigned minor)
		{
			::utsname kernel{ };
			if (::uname(&kernel) != 0)
			{
				return false;
			}

			unsigned actual_major = 0;
			unsigned actual_minor = 0;
			if (std::sscanf(kernel.release, "%u.%u", &actual_major, &actual_minor) != 2)
			{
				return false;
			}

			return actual_major > major || (actual_major == major && actual_minor >= minor);
		}

		int IoUringSetup(unsigned entries, ::io_uring_params* params)
		{
#if defined(SYS_io_uring_setup)
			return static_cast<int>(::syscall(SYS_io_uring_setup, entries, params));
#elif defined(__NR_io_uring_setup)
			return static_cast<int>(::syscall(__NR_io_uring_setup, entries, params));
#else
			(void) entries;
			(void) params;
			errno = ENOSYS;
			return -1;
#endif
		}

		int IoUringEnter(int ring_fd, unsigned to_submit, unsigned min_complete, unsigned flags)
		{
			while (true)
			{
#if defined(SYS_io_uring_enter)
				const int result = static_cast<int>(::syscall(SYS_io_uring_enter, ring_fd, to_submit, min_complete, flags, nullptr, 0));
#elif defined(__NR_io_uring_enter)
				const int result = static_cast<int>(::syscall(__NR_io_uring_enter, ring_fd, to_submit, min_complete, flags, nullptr, 0));
#else
				(void) ring_fd;
				(void) to_submit;
				(void) min_complete;
				(void) flags;
				errno = ENOSYS;
				return -1;
#endif
				if (result >= 0 || errno != EINTR)
				{
					return result;
				}
			}
		}

		std::uint32_t LoadAcquire(std::uint32_t* value)
		{
			return std::atomic_ref<std::uint32_t>(*value).load(std::memory_order_acquire);
		}

		void StoreRelease(std::uint32_t* value, std::uint32_t desired)
		{
			std::atomic_ref<std::uint32_t>(*value).store(desired, std::memory_order_release);
		}

		bool ReadOpcodeSupported()
		{
		#ifndef IORING_OP_READ
			return false;
		#else
			return KernelVersionAtLeast(5, 6);
		#endif
		}

		bool ShouldFallbackFromCompletion(int result)
		{
			return result == -EINVAL || result == -EOPNOTSUPP || result == -ENOSYS;
		}

		int BlockingReadAt(int fd, void* buf, unsigned nbytes, off_t offset)
		{
			while (true)
			{
				const ssize_t read_size = ::pread(fd, buf, nbytes, offset);
				if (read_size >= 0)
				{
					return static_cast<int>(read_size);
				}
				if (errno != EINTR)
				{
					return -errno;
				}
			}
		}
#else
		int BlockingReadAt(int, void*, unsigned, off_t)
		{
			return -ENOSYS;
		}

		bool ShouldFallbackFromCompletion(int)
		{
			return true;
		}
#endif
	} // namespace

	IoCapability IoReactor::Probe()
	{
#ifdef __linux__
		if (!KernelVersionAtLeast(5, 1))
		{
			return IoCapability::kUnavailable;
		}

		::io_uring_params params{ };
		const int ring_fd = IoUringSetup(1, &params);
		if (ring_fd < 0)
		{
			return IoCapability::kUnavailable;
		}

		::close(ring_fd);
		return IoCapability::kSupported;
#else
		return IoCapability::kUnavailable;
#endif
	}

	IoReactor::IoReactor(unsigned entries)
	{
#ifdef __linux__
		if (entries == 0 || !KernelVersionAtLeast(5, 1))
		{
			init_failed_ = true;
			return;
		}

		::io_uring_params params{ };
		ring_fd_ = IoUringSetup(entries, &params);
		if (ring_fd_ < 0)
		{
			init_failed_ = true;
			return;
		}

		sq_entries_ = params.sq_entries;
		cq_entries_ = params.cq_entries;
		sq_ring_size_ = params.sq_off.array + params.sq_entries * sizeof(std::uint32_t);
		cq_ring_size_ = params.cq_off.cqes + params.cq_entries * sizeof(::io_uring_cqe);
		if ((params.features & IORING_FEAT_SINGLE_MMAP) != 0)
		{
			if (cq_ring_size_ > sq_ring_size_)
			{
				sq_ring_size_ = cq_ring_size_;
			}
			cq_ring_size_ = sq_ring_size_;
		}

		sq_ring_ptr_ = ::mmap(nullptr, sq_ring_size_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd_, IORING_OFF_SQ_RING);
		if (sq_ring_ptr_ == MAP_FAILED)
		{
			sq_ring_ptr_ = nullptr;
			init_failed_ = true;
			Reset();
			return;
		}

		if ((params.features & IORING_FEAT_SINGLE_MMAP) != 0)
		{
			cq_ring_ptr_ = sq_ring_ptr_;
		}
		else
		{
			cq_ring_ptr_ = ::mmap(nullptr,
			    cq_ring_size_,
			    PROT_READ | PROT_WRITE,
			    MAP_SHARED | MAP_POPULATE,
			    ring_fd_,
			    IORING_OFF_CQ_RING);
			if (cq_ring_ptr_ == MAP_FAILED)
			{
				cq_ring_ptr_ = nullptr;
				init_failed_ = true;
				Reset();
				return;
			}
		}

		sqes_size_ = params.sq_entries * sizeof(::io_uring_sqe);
		sqes_ptr_ = ::mmap(nullptr, sqes_size_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd_, IORING_OFF_SQES);
		if (sqes_ptr_ == MAP_FAILED)
		{
			sqes_ptr_ = nullptr;
			init_failed_ = true;
			Reset();
			return;
		}

		sq_head_ = reinterpret_cast<std::uint32_t*>(static_cast<char*>(sq_ring_ptr_) + params.sq_off.head);
		sq_tail_ = reinterpret_cast<std::uint32_t*>(static_cast<char*>(sq_ring_ptr_) + params.sq_off.tail);
		sq_ring_mask_ = reinterpret_cast<std::uint32_t*>(static_cast<char*>(sq_ring_ptr_) + params.sq_off.ring_mask);
		sq_array_ = reinterpret_cast<std::uint32_t*>(static_cast<char*>(sq_ring_ptr_) + params.sq_off.array);
		cq_head_ = reinterpret_cast<std::uint32_t*>(static_cast<char*>(cq_ring_ptr_) + params.cq_off.head);
		cq_tail_ = reinterpret_cast<std::uint32_t*>(static_cast<char*>(cq_ring_ptr_) + params.cq_off.tail);
		cq_ring_mask_ = reinterpret_cast<std::uint32_t*>(static_cast<char*>(cq_ring_ptr_) + params.cq_off.ring_mask);
		sqes_ = reinterpret_cast<::io_uring_sqe*>(sqes_ptr_);
		cqes_ = reinterpret_cast<::io_uring_cqe*>(static_cast<char*>(cq_ring_ptr_) + params.cq_off.cqes);
#else
		(void) entries;
		init_failed_ = true;
#endif
	}

	IoReactor::~IoReactor() { Reset(); }

	IoReactor* AcquireSharedIoReactor() noexcept
	{
		static std::unique_ptr<IoReactor> reactor = []() -> std::unique_ptr<IoReactor> {
			if (IoReactor::Probe() != IoCapability::kSupported)
			{
				return nullptr;
			}

			auto instance = std::make_unique<IoReactor>();
			if (!instance->IsValid())
			{
				return nullptr;
			}
			return instance;
		}();
		return reactor.get();
	}

	IoReactor::IoReactor(IoReactor&& other) noexcept { MoveFrom(std::move(other)); }

	IoReactor& IoReactor::operator=(IoReactor&& other) noexcept
	{
		if (this != &other)
		{
			Reset();
			MoveFrom(std::move(other));
		}
		return *this;
	}

	bool IoReactor::IsValid() const noexcept { return ring_fd_ >= 0 && !init_failed_; }

	bool IoReactor::SubmitRead(int fd, void* buf, unsigned nbytes, off_t offset, uint64_t user_data)
	{
#ifdef __linux__
	#ifndef IORING_OP_READ
		(void) fd;
		(void) buf;
		(void) nbytes;
		(void) offset;
		(void) user_data;
		return false;
	#else
		std::lock_guard lock(mutex_);
		if (!IsValid() || !ReadOpcodeSupported())
		{
			return false;
		}

		const std::uint32_t head = LoadAcquire(sq_head_);
		const std::uint32_t tail = LoadAcquire(sq_tail_);
		if (tail - head >= sq_entries_)
		{
			return false;
		}

		const std::uint32_t index = tail & *sq_ring_mask_;
		::io_uring_sqe& sqe = sqes_[index];
		std::memset(&sqe, 0, sizeof(sqe));
		sqe.opcode = IORING_OP_READ;
		sqe.fd = fd;
		sqe.off = static_cast<__u64>(offset);
		sqe.addr = reinterpret_cast<__u64>(buf);
		sqe.len = nbytes;
		sqe.user_data = user_data;
		sq_array_[index] = index;
		StoreRelease(sq_tail_, tail + 1);

		if (IoUringEnter(ring_fd_, 1, 0, 0) < 0)
		{
			StoreRelease(sq_tail_, tail);
			return false;
		}
		return true;
	#endif
#else
		(void) fd;
		(void) buf;
		(void) nbytes;
		(void) offset;
		(void) user_data;
		return false;
#endif
	}

	int IoReactor::WaitCompletion(uint64_t* user_data, int* res)
	{
#ifdef __linux__
		std::lock_guard lock(mutex_);
		if (!IsValid())
		{
			return 0;
		}

		while (true)
		{
			const std::uint32_t head = LoadAcquire(cq_head_);
			const std::uint32_t tail = LoadAcquire(cq_tail_);
			if (head != tail)
			{
				const ::io_uring_cqe& cqe = cqes_[head & *cq_ring_mask_];
				if (user_data != nullptr)
				{
					*user_data = cqe.user_data;
				}
				if (res != nullptr)
				{
					*res = cqe.res;
				}
				StoreRelease(cq_head_, head + 1);
				return 1;
			}

			if (IoUringEnter(ring_fd_, 0, 1, IORING_ENTER_GETEVENTS) < 0)
			{
				return 0;
			}
		}
#else
		(void) user_data;
		(void) res;
		return 0;
#endif
	}

	bool IoReactor::IsFallback() const noexcept { return ring_fd_ < 0; }

	void IoReactor::Reset() noexcept
	{
#ifdef __linux__
		if (sqes_ptr_ != nullptr)
		{
			::munmap(sqes_ptr_, sqes_size_);
		}
		if (cq_ring_ptr_ != nullptr && cq_ring_ptr_ != sq_ring_ptr_)
		{
			::munmap(cq_ring_ptr_, cq_ring_size_);
		}
		if (sq_ring_ptr_ != nullptr)
		{
			::munmap(sq_ring_ptr_, sq_ring_size_);
		}
		if (ring_fd_ >= 0)
		{
			::close(ring_fd_);
		}
#endif

		ring_fd_ = -1;
		sq_ring_ptr_ = nullptr;
		cq_ring_ptr_ = nullptr;
		sqes_ptr_ = nullptr;
		sq_ring_size_ = 0;
		cq_ring_size_ = 0;
		sqes_size_ = 0;
		sq_entries_ = 0;
		cq_entries_ = 0;
		sq_head_ = nullptr;
		sq_tail_ = nullptr;
		sq_ring_mask_ = nullptr;
		sq_array_ = nullptr;
		cq_head_ = nullptr;
		cq_tail_ = nullptr;
		cq_ring_mask_ = nullptr;
		sqes_ = nullptr;
		cqes_ = nullptr;
	}

	void IoReactor::MoveFrom(IoReactor&& other) noexcept
	{
		ring_fd_ = other.ring_fd_;
		sq_ring_ptr_ = other.sq_ring_ptr_;
		cq_ring_ptr_ = other.cq_ring_ptr_;
		sqes_ptr_ = other.sqes_ptr_;
		sq_ring_size_ = other.sq_ring_size_;
		cq_ring_size_ = other.cq_ring_size_;
		sqes_size_ = other.sqes_size_;
		sq_entries_ = other.sq_entries_;
		cq_entries_ = other.cq_entries_;
		sq_head_ = other.sq_head_;
		sq_tail_ = other.sq_tail_;
		sq_ring_mask_ = other.sq_ring_mask_;
		sq_array_ = other.sq_array_;
		cq_head_ = other.cq_head_;
		cq_tail_ = other.cq_tail_;
		cq_ring_mask_ = other.cq_ring_mask_;
		sqes_ = other.sqes_;
		cqes_ = other.cqes_;
		init_failed_ = other.init_failed_;

		other.ring_fd_ = -1;
		other.sq_ring_ptr_ = nullptr;
		other.cq_ring_ptr_ = nullptr;
		other.sqes_ptr_ = nullptr;
		other.sq_ring_size_ = 0;
		other.cq_ring_size_ = 0;
		other.sqes_size_ = 0;
		other.sq_entries_ = 0;
		other.cq_entries_ = 0;
		other.sq_head_ = nullptr;
		other.sq_tail_ = nullptr;
		other.sq_ring_mask_ = nullptr;
		other.sq_array_ = nullptr;
		other.cq_head_ = nullptr;
		other.cq_tail_ = nullptr;
		other.cq_ring_mask_ = nullptr;
		other.sqes_ = nullptr;
		other.cqes_ = nullptr;
		other.init_failed_ = false;
	}

	IoReadDispatcher::IoReadDispatcher(BlockingExecutor& blocking_executor, unsigned entries)
	    : blocking_executor_(&blocking_executor)
	    , capability_(IoReactor::Probe())
	{
		if (capability_ == IoCapability::kSupported)
		{
			auto reactor = std::make_unique<IoReactor>(entries);
			if (reactor->IsValid())
			{
				reactor_ = std::move(reactor);
			}
			else
			{
				RecordFallback();
			}
		}
	}

	IoReadDispatcher::IoReadDispatcher(BlockingExecutor& blocking_executor,
	    IoCapability capability,
	    std::unique_ptr<IIoReactor> reactor)
	    : blocking_executor_(&blocking_executor)
	    , capability_(capability)
	    , reactor_(std::move(reactor))
	{
		if (capability_ == IoCapability::kSupported && !HasReactor())
		{
			RecordFallback();
		}
	}

	bool IoReadDispatcher::HasReactor() const noexcept
	{
		return capability_ == IoCapability::kSupported && reactor_ != nullptr && reactor_->IsValid();
	}

	void IoReadDispatcher::SubmitRead(int fd,
	    void* buf,
	    unsigned nbytes,
	    off_t offset,
	    uint64_t user_data,
	    std::function<void(uint64_t, int)> completion)
	{
		if (!HasReactor())
		{
			RecordFallback();
			SubmitBlockingRead(fd, buf, nbytes, offset, user_data, std::move(completion));
			return;
		}

		if (!reactor_->SubmitRead(fd, buf, nbytes, offset, user_data))
		{
			RecordFallback();
			SubmitBlockingRead(fd, buf, nbytes, offset, user_data, std::move(completion));
			return;
		}

		blocking_executor_->Submit([this, fd, buf, nbytes, offset, user_data, completion = std::move(completion)]() mutable {
			uint64_t completed_user_data = 0;
			int result = 0;
			const int consumed = reactor_->WaitCompletion(&completed_user_data, &result);
			if (consumed <= 0 || ShouldFallbackFromCompletion(result))
			{
				RecordFallback();
				if (completion)
				{
					completion(user_data, BlockingReadAt(fd, buf, nbytes, offset));
				}
				return;
			}

			if (completion)
			{
				completion(completed_user_data, result);
			}
		});
	}

	void IoReadDispatcher::RecordFallback() const noexcept
	{
		RuntimeMetrics::Instance().fallback_to_blocking_count.fetch_add(kFallbackIncrement, std::memory_order_relaxed);
	}

	void IoReadDispatcher::SubmitBlockingRead(int fd,
	    void* buf,
	    unsigned nbytes,
	    off_t offset,
	    uint64_t user_data,
	    std::function<void(uint64_t, int)> completion)
	{
		blocking_executor_->Submit([fd, buf, nbytes, offset, user_data, completion = std::move(completion)]() mutable {
			if (completion)
			{
				completion(user_data, BlockingReadAt(fd, buf, nbytes, offset));
			}
		});
	}

} // namespace prism
