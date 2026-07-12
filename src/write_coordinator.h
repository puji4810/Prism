#ifndef PRISM_WRITE_COORDINATOR_H
#define PRISM_WRITE_COORDINATOR_H

#include "async_write_op.h"
#include "scheduler.h"

#include <cstddef>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <vector>

namespace prism
{
	class DBImpl;
	class SerialExecutor;

	class WriteCoordinator
	{
	public:
		WriteCoordinator(DBImpl& db, SerialExecutor& executor);

		Status SubmitSync(WriteOptions options, WriteBatch batch);
		AsyncWriteOp SubmitAsync(WriteOptions options, WriteBatch batch);
		void Shutdown();

	private:
		friend struct AsyncWriteOp::Awaiter;

		void Enqueue(WriteRequestState* request);
		void Drain();
		void CompleteGroup(std::vector<WriteRequestState*> group, Status status);
		bool FinishGroup();
		std::vector<WriteRequestState*> SelectGroupLocked();

		static constexpr std::size_t kMaxGroupRequests = 128;
		static constexpr std::size_t kMaxGroupBytes = 1 * 1024 * 1024;

		DBImpl& db_;
		SerialExecutor& executor_;
		std::mutex mutex_;
		std::condition_variable idle_cv_;
		std::deque<WriteRequestState*> queue_;
		bool drain_scheduled_ = false;
		bool group_in_flight_ = false;
		bool shutting_down_ = false;
	};
} // namespace prism

#endif // PRISM_WRITE_COORDINATOR_H
