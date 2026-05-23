#ifndef PRISM_IO_REACTOR_H
#define PRISM_IO_REACTOR_H

#include <cstdint>
#include <condition_variable>
#include <functional>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <thread>
#include <sys/types.h>

#ifdef __linux__
#include <linux/io_uring.h>
#endif

namespace prism
{
	class BlockingExecutor;

	enum class IoCapability
	{
		kSupported,
		kUnavailable,
	};

	class IoReactor final
	{
	public:
		static IoCapability Probe();

		explicit IoReactor(unsigned entries = 256);
		~IoReactor();

		IoReactor(const IoReactor&) = delete;
		IoReactor& operator=(const IoReactor&) = delete;
		IoReactor(IoReactor&& other) noexcept;
		IoReactor& operator=(IoReactor&& other) noexcept;

		bool IsValid() const noexcept;
		bool SubmitRead(int fd, void* buf, unsigned nbytes, off_t offset, uint64_t user_data);
		bool SubmitWrite(int fd, const void* buf, unsigned nbytes, off_t offset, uint64_t user_data);
		int WaitCompletion(uint64_t* user_data, int* res);
		bool IsFallback() const noexcept;

	private:
		void Reset() noexcept;
		void MoveFrom(IoReactor&& other) noexcept;

		int ring_fd_{ -1 };
		void* sq_ring_ptr_{ nullptr };
		void* cq_ring_ptr_{ nullptr };
		void* sqes_ptr_{ nullptr };
		unsigned sq_ring_size_{ 0 };
		unsigned cq_ring_size_{ 0 };
		unsigned sqes_size_{ 0 };
		std::uint32_t sq_entries_{ 0 };
		std::uint32_t cq_entries_{ 0 };
		std::uint32_t* sq_head_{ nullptr };
		std::uint32_t* sq_tail_{ nullptr };
		std::uint32_t* sq_ring_mask_{ nullptr };
		std::uint32_t* sq_array_{ nullptr };
		std::uint32_t* cq_head_{ nullptr };
		std::uint32_t* cq_tail_{ nullptr };
		std::uint32_t* cq_ring_mask_{ nullptr };
#ifdef __linux__
		io_uring_sqe* sqes_{ nullptr };
		io_uring_cqe* cqes_{ nullptr };
#else
		void* sqes_{ nullptr };
		void* cqes_{ nullptr };
#endif
		bool init_failed_{ false };
		mutable std::mutex mutex_;
	};

	// Returns a process-lifetime shared reactor when io_uring is supported and
	// initialization succeeds, otherwise nullptr.
	IoReactor* AcquireSharedIoReactor() noexcept;

	class IoDispatcher final
	{
	public:
		struct TestReactor
		{
			std::function<bool()> is_valid;
			std::function<bool(int, void*, unsigned, off_t, uint64_t)> submit_read;
			std::function<bool(int, const void*, unsigned, off_t, uint64_t)> submit_write;
			std::function<bool(uint64_t)> submit_noop;
			std::function<int(uint64_t*, int*)> wait_completion;
		};

		explicit IoDispatcher(BlockingExecutor& blocking_executor, unsigned entries = 256);
		IoDispatcher(BlockingExecutor& blocking_executor,
		    IoCapability capability,
		    TestReactor reactor);
		~IoDispatcher();

		IoDispatcher(const IoDispatcher&) = delete;
		IoDispatcher& operator=(const IoDispatcher&) = delete;
		IoDispatcher(IoDispatcher&&) = delete;
		IoDispatcher& operator=(IoDispatcher&&) = delete;

		bool HasReactor() const noexcept;

		void SubmitRead(int fd,
		    void* buf,
		    unsigned nbytes,
		    off_t offset,
		    uint64_t user_data,
		    std::function<void(uint64_t, int)> completion);
		void SubmitWrite(int fd,
		    const void* buf,
		    unsigned nbytes,
		    off_t offset,
		    uint64_t user_data,
		    std::function<void(uint64_t, int)> completion);

	private:
		enum class Operation
		{
			kRead,
			kWrite,
		};

		struct RequestState;
		struct PendingRequest
		{
			uint64_t request_id;
			std::shared_ptr<RequestState> state;
		};

		void RecordFallback() const noexcept;
		void SubmitBlockingRead(int fd,
		    void* buf,
		    unsigned nbytes,
		    off_t offset,
		    uint64_t user_data,
		    std::function<void(uint64_t, int)> completion);
		void SubmitBlockingWrite(int fd,
		    const void* buf,
		    unsigned nbytes,
		    off_t offset,
		    uint64_t user_data,
		    std::function<void(uint64_t, int)> completion);
		void SubmitRequest(Operation operation,
		    int fd,
		    const void* buf,
		    unsigned nbytes,
		    off_t offset,
		    uint64_t user_data,
		    std::function<void(uint64_t, int)> completion);
		bool ReactorIsValid() const noexcept;
		bool ReactorSubmitRead(int fd, void* buf, unsigned nbytes, off_t offset, uint64_t user_data);
		bool ReactorSubmitWrite(int fd, const void* buf, unsigned nbytes, off_t offset, uint64_t user_data);
		int ReactorWaitCompletion(uint64_t* user_data, int* res);
		void PumpLoop();

		BlockingExecutor* blocking_executor_;
		IoCapability capability_;
		std::unique_ptr<IoReactor> reactor_;
		std::optional<TestReactor> test_reactor_;
		std::mutex queue_mutex_;
		std::condition_variable queue_cv_;
		std::deque<PendingRequest> pending_requests_;
		std::unordered_map<uint64_t, std::shared_ptr<RequestState>> in_flight_;
		std::jthread pump_thread_;
		std::uint64_t next_request_id_{ 1 };
		bool stopping_{ false };
	};

} // namespace prism

#endif // PRISM_IO_REACTOR_H
