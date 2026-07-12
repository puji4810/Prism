#include "io_reactor.h"

#include "async_runtime.h"
#include "port/port_config.h"
#include "runtime_metrics.h"

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <memory>
#include <unordered_map>
#include <utility>

#ifdef __linux__
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/utsname.h>
#include <unistd.h>
#endif

	namespace prism
{
	struct IoDispatcher::RequestState
	{
		Operation operation{ Operation::kRead };
		uint64_t original_user_data{ 0 };
		int fd{ -1 };
		const void* buf{ nullptr };
		unsigned nbytes{ 0 };
		off_t offset{ 0 };
		std::move_only_function<void(uint64_t, int)> completion;
	};

	namespace
	{
		constexpr std::size_t kFallbackIncrement = 1;
		constexpr std::size_t kMaxSubmitBatch = 32;
		constexpr unsigned kProbeOpcodeCount = 256;

#ifndef IORING_OP_READ
		constexpr std::uint8_t kIoUringOpRead = 22;
#else
		constexpr std::uint8_t kIoUringOpRead = IORING_OP_READ;
#endif

#ifndef IORING_OP_WRITE
		constexpr std::uint8_t kIoUringOpWrite = 23;
#else
		constexpr std::uint8_t kIoUringOpWrite = IORING_OP_WRITE;
#endif

#ifndef IORING_OP_FSYNC
		constexpr std::uint8_t kIoUringOpFsync = 3;
#else
		constexpr std::uint8_t kIoUringOpFsync = IORING_OP_FSYNC;
#endif

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
			(void)entries;
			(void)params;
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
				(void)ring_fd;
				(void)to_submit;
				(void)min_complete;
				(void)flags;
				errno = ENOSYS;
				return -1;
#endif
				if (result >= 0 || errno != EINTR)
				{
					return result;
				}
			}
		}

		int IoUringRegister(int ring_fd, unsigned opcode, const void* arg, unsigned nr_args)
		{
			while (true)
			{
#if defined(SYS_io_uring_register)
				const int result = static_cast<int>(::syscall(SYS_io_uring_register, ring_fd, opcode, arg, nr_args));
#elif defined(__NR_io_uring_register)
				const int result = static_cast<int>(::syscall(__NR_io_uring_register, ring_fd, opcode, arg, nr_args));
#else
				(void)ring_fd;
				(void)opcode;
				(void)arg;
				(void)nr_args;
				errno = ENOSYS;
				return -1;
#endif
				if (result >= 0 || errno != EINTR)
				{
					return result;
				}
			}
		}

		std::uint32_t LoadAcquire(std::uint32_t* value) { return std::atomic_ref<std::uint32_t>(*value).load(std::memory_order_acquire); }

		void StoreRelease(std::uint32_t* value, std::uint32_t desired)
		{
			std::atomic_ref<std::uint32_t>(*value).store(desired, std::memory_order_release);
		}

		bool OpcodeSupported(std::uint8_t opcode)
		{
#if defined(IORING_REGISTER_PROBE)
			::io_uring_params params{ };
			const int ring_fd = IoUringSetup(1, &params);
			if (ring_fd < 0)
			{
				return false;
			}

			const std::size_t probe_size = sizeof(::io_uring_probe) + kProbeOpcodeCount * sizeof(::io_uring_probe_op);
			auto probe_storage = std::make_unique<std::byte[]>(probe_size);
			auto* probe = reinterpret_cast<::io_uring_probe*>(probe_storage.get());
			std::memset(probe, 0, probe_size);

			const int register_result = IoUringRegister(ring_fd, IORING_REGISTER_PROBE, probe, kProbeOpcodeCount);
			::close(ring_fd);
			if (register_result < 0)
			{
				return true;
			}

			for (unsigned i = 0; i < probe->ops_len; ++i)
			{
				const auto& op = probe->ops[i];
				if (op.op == opcode)
				{
					return (op.flags & IO_URING_OP_SUPPORTED) != 0;
				}
			}
			return false;
#else
			return true;
#endif
		}

		bool ReadOpcodeSupported() { return OpcodeSupported(kIoUringOpRead); }
		bool WriteOpcodeSupported() { return OpcodeSupported(kIoUringOpWrite); }
		bool FsyncOpcodeSupported() { return OpcodeSupported(kIoUringOpFsync); }

		bool ShouldFallbackFromCompletion(int result) { return result == -EINVAL || result == -EOPNOTSUPP || result == -ENOSYS; }

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

		int BlockingWriteAt(int fd, const void* buf, unsigned nbytes, off_t offset)
		{
			const auto* data = static_cast<const std::byte*>(buf);
			unsigned remaining = nbytes;
			off_t current_offset = offset;
			while (remaining > 0)
			{
				const ssize_t write_size = ::pwrite(fd, data, remaining, current_offset);
				if (write_size > 0)
				{
					data += write_size;
					remaining -= static_cast<unsigned>(write_size);
					current_offset += write_size;
					continue;
				}
				if (write_size == 0)
				{
					return -EIO;
				}
				if (errno != EINTR)
				{
					return -errno;
				}
			}
			return static_cast<int>(nbytes);
		}

		int BlockingFsync(int fd)
		{
			while (true)
			{
#if HAVE_FDATASYNC
				const int result = ::fdatasync(fd);
#else
				const int result = ::fsync(fd);
#endif
				if (result == 0)
				{
					return 0;
				}
				if (errno != EINTR)
				{
					return -errno;
				}
			}
		}
#else
		int BlockingReadAt(int, void*, unsigned, off_t) { return -ENOSYS; }
		int BlockingWriteAt(int, const void*, unsigned, off_t) { return -ENOSYS; }
		int BlockingFsync(int) { return -ENOSYS; }

		bool ShouldFallbackFromCompletion(int) { return true; }
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
			cq_ring_ptr_ = ::mmap(nullptr, cq_ring_size_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd_, IORING_OFF_CQ_RING);
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
		(void)entries;
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
		sqe.opcode = kIoUringOpRead;
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
#else
		(void)fd;
		(void)buf;
		(void)nbytes;
		(void)offset;
		(void)user_data;
		return false;
#endif
	}

	bool IoReactor::SubmitWrite(int fd, const void* buf, unsigned nbytes, off_t offset, uint64_t user_data)
	{
#ifdef __linux__
		std::lock_guard lock(mutex_);
		if (!IsValid() || !WriteOpcodeSupported())
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
		sqe.opcode = kIoUringOpWrite;
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
#else
		(void)fd;
		(void)buf;
		(void)nbytes;
		(void)offset;
		(void)user_data;
		return false;
#endif
	}

	bool IoReactor::SubmitFsync(int fd, uint64_t user_data)
	{
#ifdef __linux__
		std::lock_guard lock(mutex_);
		if (!IsValid() || !FsyncOpcodeSupported())
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
		sqe.opcode = kIoUringOpFsync;
		sqe.fd = fd;
		sqe.user_data = user_data;
		sq_array_[index] = index;
		StoreRelease(sq_tail_, tail + 1);

		if (IoUringEnter(ring_fd_, 1, 0, 0) < 0)
		{
			StoreRelease(sq_tail_, tail);
			return false;
		}
		return true;
#else
		(void)fd;
		(void)user_data;
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
		(void)user_data;
		(void)res;
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

	IoDispatcher::IoDispatcher(BlockingExecutor& blocking_executor, unsigned entries)
	    : blocking_executor_(&blocking_executor)
	    , capability_(IoReactor::Probe())
	{
		if (capability_ == IoCapability::kSupported)
		{
			auto reactor = std::make_unique<IoReactor>(entries);
			if (reactor->IsValid())
			{
				reactor_ = std::move(reactor);
				pump_thread_ = std::jthread([this] { PumpLoop(); });
			}
			else
			{
				RecordFallback();
			}
		}
	}

	IoDispatcher::IoDispatcher(BlockingExecutor& blocking_executor, IoCapability capability, TestReactor reactor)
	    : blocking_executor_(&blocking_executor)
	    , capability_(capability)
	    , test_reactor_(std::move(reactor))
	{
		if (capability_ == IoCapability::kSupported && !HasReactor())
		{
			RecordFallback();
		}
		else if (HasReactor())
		{
			pump_thread_ = std::jthread([this] { PumpLoop(); });
		}
	}

	IoDispatcher::~IoDispatcher()
	{
		{
			std::lock_guard lock(queue_mutex_);
			stopping_ = true;
		}
		queue_cv_.notify_all();
	}

	bool IoDispatcher::HasReactor() const noexcept
	{
		return capability_ == IoCapability::kSupported && ReactorIsValid();
	}

	void IoDispatcher::SubmitRead(
	    int fd, void* buf, unsigned nbytes, off_t offset, uint64_t user_data, std::move_only_function<void(uint64_t, int)> completion)
	{
		SubmitRequest(Operation::kRead, fd, buf, nbytes, offset, user_data, std::move(completion));
	}

	void IoDispatcher::SubmitWrite(
	    int fd, const void* buf, unsigned nbytes, off_t offset, uint64_t user_data, std::move_only_function<void(uint64_t, int)> completion)
	{
		SubmitRequest(Operation::kWrite, fd, buf, nbytes, offset, user_data, std::move(completion));
	}

	void IoDispatcher::SubmitFsync(int fd, uint64_t user_data, std::move_only_function<void(uint64_t, int)> completion)
	{
		SubmitRequest(Operation::kFsync, fd, nullptr, 0, 0, user_data, std::move(completion));
	}

	void IoDispatcher::SubmitRequest(Operation operation,
	    int fd,
	    const void* buf,
	    unsigned nbytes,
	    off_t offset,
	    uint64_t user_data,
	    std::move_only_function<void(uint64_t, int)> completion)
	{
		if (!HasReactor())
		{
			RecordFallback();
			if (operation == Operation::kRead)
			{
				SubmitBlockingRead(fd, const_cast<void*>(buf), nbytes, offset, user_data, std::move(completion));
			}
			else if (operation == Operation::kWrite)
			{
				SubmitBlockingWrite(fd, buf, nbytes, offset, user_data, std::move(completion));
			}
			else
			{
				SubmitBlockingFsync(fd, user_data, std::move(completion));
			}
			return;
		}

		auto state = std::make_shared<RequestState>();
		state->operation = operation;
		state->original_user_data = user_data;
		state->fd = fd;
		state->buf = buf;
		state->nbytes = nbytes;
		state->offset = offset;
		state->completion = std::move(completion);
		{
			std::lock_guard lock(queue_mutex_);
			pending_requests_.push_back(PendingRequest{ next_request_id_++, state });
		}
		queue_cv_.notify_one();
	}

	bool IoDispatcher::ReactorIsValid() const noexcept
	{
		if (reactor_ != nullptr)
		{
			return reactor_->IsValid();
		}
		return test_reactor_.has_value() && test_reactor_->is_valid && test_reactor_->is_valid();
	}

	bool IoDispatcher::ReactorSubmitRead(int fd, void* buf, unsigned nbytes, off_t offset, uint64_t user_data)
	{
		return reactor_ != nullptr ? reactor_->SubmitRead(fd, buf, nbytes, offset, user_data)
		                           : test_reactor_->submit_read(fd, buf, nbytes, offset, user_data);
	}

	bool IoDispatcher::ReactorSubmitWrite(int fd, const void* buf, unsigned nbytes, off_t offset, uint64_t user_data)
	{
		return reactor_ != nullptr ? reactor_->SubmitWrite(fd, buf, nbytes, offset, user_data)
		                           : test_reactor_->submit_write(fd, buf, nbytes, offset, user_data);
	}

	bool IoDispatcher::ReactorSubmitFsync(int fd, uint64_t user_data)
	{
		if (reactor_ != nullptr)
		{
			return reactor_->SubmitFsync(fd, user_data);
		}
		return test_reactor_->submit_fsync && test_reactor_->submit_fsync(fd, user_data);
	}

	int IoDispatcher::ReactorWaitCompletion(uint64_t* user_data, int* res)
	{
		return reactor_ != nullptr ? reactor_->WaitCompletion(user_data, res) : test_reactor_->wait_completion(user_data, res);
	}

	void IoDispatcher::PumpLoop()
	{
		while (true)
		{
			std::deque<PendingRequest> batch;
			{
				std::unique_lock lock(queue_mutex_);
				queue_cv_.wait(lock, [this] { return stopping_ || !pending_requests_.empty(); });
				if (stopping_ && pending_requests_.empty())
				{
					return;
				}
				while (!pending_requests_.empty() && batch.size() < kMaxSubmitBatch)
				{
					batch.push_back(std::move(pending_requests_.front()));
					pending_requests_.pop_front();
				}
			}

			for (auto& request : batch)
			{
				{
					std::lock_guard lock(queue_mutex_);
					in_flight_.emplace(request.request_id, request.state);
				}
				bool submitted = false;
				if (request.state->operation == Operation::kRead)
				{
					submitted = ReactorSubmitRead(request.state->fd,
					    const_cast<void*>(request.state->buf),
					    request.state->nbytes,
					    request.state->offset,
					    request.request_id);
				}
				else if (request.state->operation == Operation::kWrite)
				{
					submitted = ReactorSubmitWrite(
					    request.state->fd, request.state->buf, request.state->nbytes, request.state->offset, request.request_id);
				}
				else
				{
					submitted = ReactorSubmitFsync(request.state->fd, request.request_id);
				}
				if (!submitted)
				{
					RecordFallback();
					int result = 0;
					if (request.state->operation == Operation::kRead)
					{
						result = BlockingReadAt(
						    request.state->fd, const_cast<void*>(request.state->buf), request.state->nbytes, request.state->offset);
					}
					else if (request.state->operation == Operation::kWrite)
					{
						result = BlockingWriteAt(request.state->fd, request.state->buf, request.state->nbytes, request.state->offset);
					}
					else
					{
						result = BlockingFsync(request.state->fd);
					}
					if (request.state->completion)
					{
						request.state->completion(request.state->original_user_data, result);
					}
					{
						std::lock_guard lock(queue_mutex_);
						in_flight_.erase(request.request_id);
					}
					continue;
				}
			}

			while (true)
			{
				{
					std::lock_guard lock(queue_mutex_);
					if (in_flight_.empty())
					{
						break;
					}
				}

				uint64_t completed_request_id = 0;
				int result = 0;
				const int consumed = ReactorWaitCompletion(&completed_request_id, &result);
				if (consumed <= 0)
				{
					break;
				}

				std::shared_ptr<RequestState> state;
				{
					std::lock_guard lock(queue_mutex_);
					const auto it = in_flight_.find(completed_request_id);
					if (it != in_flight_.end())
					{
						state = std::move(it->second);
						in_flight_.erase(it);
					}
				}

				if (!state)
				{
					continue;
				}

				int completed_result = result;
				if (ShouldFallbackFromCompletion(result))
				{
					if (state->operation == Operation::kRead)
					{
						completed_result = BlockingReadAt(state->fd, const_cast<void*>(state->buf), state->nbytes, state->offset);
					}
					else if (state->operation == Operation::kWrite)
					{
						completed_result = BlockingWriteAt(state->fd, state->buf, state->nbytes, state->offset);
					}
					else
					{
						completed_result = BlockingFsync(state->fd);
					}
				}
				if (state->completion)
				{
					state->completion(state->original_user_data, completed_result);
				}

				{
					std::lock_guard lock(queue_mutex_);
					if (in_flight_.empty())
					{
						break;
					}
				}
			}
		}
	}

	void IoDispatcher::RecordFallback() const noexcept
	{
#ifdef PRISM_RUNTIME_METRICS
		RuntimeMetrics::Instance().fallback_to_blocking_count.fetch_add(kFallbackIncrement, std::memory_order_relaxed);
#endif
	}

	void IoDispatcher::SubmitBlockingRead(
	    int fd, void* buf, unsigned nbytes, off_t offset, uint64_t user_data, std::move_only_function<void(uint64_t, int)> completion)
	{
		blocking_executor_->Submit([fd, buf, nbytes, offset, user_data, completion = std::move(completion)]() mutable {
			if (completion)
			{
				completion(user_data, BlockingReadAt(fd, buf, nbytes, offset));
			}
		});
	}

	void IoDispatcher::SubmitBlockingWrite(
	    int fd, const void* buf, unsigned nbytes, off_t offset, uint64_t user_data, std::move_only_function<void(uint64_t, int)> completion)
	{
		blocking_executor_->Submit([fd, buf, nbytes, offset, user_data, completion = std::move(completion)]() mutable {
			if (completion)
			{
				completion(user_data, BlockingWriteAt(fd, buf, nbytes, offset));
			}
		});
	}

	void IoDispatcher::SubmitBlockingFsync(int fd, uint64_t user_data, std::move_only_function<void(uint64_t, int)> completion)
	{
		blocking_executor_->Submit([fd, user_data, completion = std::move(completion)]() mutable {
			if (completion)
			{
				completion(user_data, BlockingFsync(fd));
			}
		});
	}

} // namespace prism
