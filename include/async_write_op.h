#ifndef PRISM_ASYNC_WRITE_OP_H
#define PRISM_ASYNC_WRITE_OP_H

#include "options.h"
#include "status.h"
#include "write_batch.h"

#include <atomic>
#include <condition_variable>
#include <coroutine>
#include <memory>
#include <mutex>

namespace prism
{
	class WriteCoordinator;

	struct WriteRequestState
	{
		struct BlockingWait
		{
			std::mutex mutex;
			std::condition_variable cv;
			bool done = false;
		};

		static constexpr int kSubmitting = 0;
		static constexpr int kCompleted = 1;
		static constexpr int kSuspended = 2;

		WriteRequestState(WriteOptions write_options, WriteBatch write_batch, BlockingWait* blocking_wait = nullptr);

		void WaitBlocking();
		bool TrySuspend() noexcept;
		void Complete(Status completion_status);

		WriteOptions options;
		WriteBatch batch;
		Status status;
		std::atomic<int> state{ kSubmitting };
		std::coroutine_handle<> continuation;
		BlockingWait* blocking_wait = nullptr;
	};

	class AsyncWriteOp
	{
	public:
		struct Awaiter;

		AsyncWriteOp(WriteCoordinator& coordinator, WriteOptions options, WriteBatch batch);
		AsyncWriteOp(WriteCoordinator& coordinator, void* keep_alive, void (*release_keep_alive)(void*), WriteOptions options, WriteBatch batch);
		~AsyncWriteOp();

		AsyncWriteOp(const AsyncWriteOp&) = delete;
		AsyncWriteOp& operator=(const AsyncWriteOp&) = delete;
		AsyncWriteOp(AsyncWriteOp&&) noexcept;
		AsyncWriteOp& operator=(AsyncWriteOp&&) noexcept;

		Awaiter operator co_await() && noexcept;

	private:
		WriteCoordinator* coordinator_ = nullptr;
		std::unique_ptr<WriteRequestState> state_;
		void* keep_alive_ = nullptr;
		void (*release_keep_alive_)(void*) = nullptr;
	};

	struct AsyncWriteOp::Awaiter
	{
		WriteCoordinator* coordinator = nullptr;
		std::unique_ptr<WriteRequestState> state;
		void* keep_alive = nullptr;
		void (*release_keep_alive)(void*) = nullptr;

		bool await_ready() const noexcept;
		bool await_suspend(std::coroutine_handle<> handle);
		Status await_resume();
		~Awaiter();
	};
} // namespace prism

#endif // PRISM_ASYNC_WRITE_OP_H
