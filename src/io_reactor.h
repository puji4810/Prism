#ifndef PRISM_IO_REACTOR_H
#define PRISM_IO_REACTOR_H

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
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

	class IIoReactor
	{
	public:
		virtual ~IIoReactor() = default;

		virtual bool IsValid() const noexcept = 0;
		virtual bool SubmitRead(int fd, void* buf, unsigned nbytes, off_t offset, uint64_t user_data) = 0;
		virtual int WaitCompletion(uint64_t* user_data, int* res) = 0;
	};

	class IoReactor final: public IIoReactor
	{
	public:
		static IoCapability Probe();

		explicit IoReactor(unsigned entries = 256);
		~IoReactor() override;

		IoReactor(const IoReactor&) = delete;
		IoReactor& operator=(const IoReactor&) = delete;
		IoReactor(IoReactor&& other) noexcept;
		IoReactor& operator=(IoReactor&& other) noexcept;

		bool IsValid() const noexcept override;
		bool SubmitRead(int fd, void* buf, unsigned nbytes, off_t offset, uint64_t user_data) override;
		int WaitCompletion(uint64_t* user_data, int* res) override;

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

	class IoReadDispatcher final
	{
	public:
		explicit IoReadDispatcher(BlockingExecutor& blocking_executor, unsigned entries = 256);
		IoReadDispatcher(BlockingExecutor& blocking_executor,
		    IoCapability capability,
		    std::unique_ptr<IIoReactor> reactor);

		IoReadDispatcher(const IoReadDispatcher&) = delete;
		IoReadDispatcher& operator=(const IoReadDispatcher&) = delete;
		IoReadDispatcher(IoReadDispatcher&&) = delete;
		IoReadDispatcher& operator=(IoReadDispatcher&&) = delete;

		bool HasReactor() const noexcept;

		void SubmitRead(int fd,
		    void* buf,
		    unsigned nbytes,
		    off_t offset,
		    uint64_t user_data,
		    std::function<void(uint64_t, int)> completion);

	private:
		void RecordFallback() const noexcept;
		void SubmitBlockingRead(int fd,
		    void* buf,
		    unsigned nbytes,
		    off_t offset,
		    uint64_t user_data,
		    std::function<void(uint64_t, int)> completion);

		BlockingExecutor* blocking_executor_;
		IoCapability capability_;
		std::unique_ptr<IIoReactor> reactor_;
	};

} // namespace prism

#endif // PRISM_IO_REACTOR_H
