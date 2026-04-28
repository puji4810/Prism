#ifndef PRISM_TASK_SCOPE_H
#define PRISM_TASK_SCOPE_H

#include "runtime_executor.h"
#include "runtime_metrics.h"

#include <any>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <type_traits>
#include <utility>
#include <vector>

namespace prism
{
	class StopToken;

	class StopSource
	{
	public:
		struct StopState;

		StopSource();

		void RequestStop() noexcept;
		bool StopRequested() const noexcept;
		StopToken Token() const;
		StopToken Token(StopToken parent) const;

	private:
		std::shared_ptr<StopState> state_;
	};

	class StopToken
	{
	public:
		StopToken() = default;

		bool StopRequested() const noexcept;
		bool CheckStop() const noexcept;

		struct ChainNode;

	private:
		explicit StopToken(std::shared_ptr<const ChainNode> chain)
		    : chain_(std::move(chain))
		{
		}

		std::shared_ptr<const ChainNode> chain_;

		friend class StopSource;
		friend class TaskScope;
	};

	class Quarantine
	{
	public:
		enum class EntryKind
		{
			kCancelled,
			kValue,
			kException,
			kUnit,
		};

		struct Entry
		{
			EntryKind kind;
			std::any payload;
		};

		void StoreCancelled();
		void StoreUnit();
		void StoreException(std::exception_ptr exception);

		template <typename T>
		void StoreValue(T&& value)
		{
			std::lock_guard lock(mutex_);
			entries_.push_back(Entry{ EntryKind::kValue, std::any(std::forward<T>(value)) });
		}

		std::size_t Size() const;
		std::size_t Count(EntryKind kind) const;
		std::vector<Entry> Snapshot() const;

	private:
		mutable std::mutex mutex_;
		std::vector<Entry> entries_;
	};

	namespace detail
	{
		template <typename Fn>
		decltype(auto) InvokeWithOptionalToken(Fn&& fn, StopToken token)
		{
			if constexpr (std::is_invocable_v<Fn, StopToken>)
			{
				return std::invoke(std::forward<Fn>(fn), token);
			}
			else
			{
				return std::invoke(std::forward<Fn>(fn));
			}
		}

		template <typename Fn>
		auto InvokeResultType(int) -> std::type_identity<std::invoke_result_t<Fn, StopToken>>;

		template <typename Fn>
		auto InvokeResultType(...) -> std::type_identity<std::invoke_result_t<Fn>>;

		template <typename Fn>
		using InvokeResult = typename decltype(InvokeResultType<Fn>(0))::type;
	}

	template <typename T>
	class OperationState
	{
	public:
		using ApplyFn = std::function<void(T)>;

		OperationState(StopToken token, Quarantine& quarantine, ApplyFn apply = {})
		    : token_(std::move(token))
		    , quarantine_(&quarantine)
		    , apply_(std::move(apply))
		{
		}

		bool TryStart()
		{
			if (token_.StopRequested())
			{
				std::lock_guard lock(mutex_);
				if (!completed_)
				{
					completed_ = true;
					quarantined_ = true;
					RuntimeMetrics::Instance().cancelled_before_start.fetch_add(1, std::memory_order_relaxed);
					quarantine_->StoreCancelled();
				}
				return false;
			}

			std::lock_guard lock(mutex_);
			if (completed_)
			{
				return false;
			}
			if (cancelled_ || token_.StopRequested())
			{
				completed_ = true;
				quarantined_ = true;
				RuntimeMetrics::Instance().cancelled_before_start.fetch_add(1, std::memory_order_relaxed);
				quarantine_->StoreCancelled();
				return false;
			}
			started_ = true;
			return true;
		}

		void Cancel()
		{
			std::lock_guard lock(mutex_);
			cancelled_ = true;
			if (!started_ && !completed_)
			{
				completed_ = true;
				quarantined_ = true;
				RuntimeMetrics::Instance().cancelled_before_start.fetch_add(1, std::memory_order_relaxed);
				quarantine_->StoreCancelled();
			}
		}

		void Complete(T value)
		{
			ApplyFn apply;
			bool quarantine_value = false;
			{
				std::lock_guard lock(mutex_);
				if (completed_)
				{
					return;
				}
				if (cancelled_ || token_.StopRequested())
				{
					completed_ = true;
					quarantined_ = true;
					quarantine_value = true;
				}
				else
				{
					apply = apply_;
				}
			}

			if (quarantine_value)
			{
				RuntimeMetrics::Instance().late_completion_quarantined.fetch_add(1, std::memory_order_relaxed);
				quarantine_->StoreValue(std::move(value));
				return;
			}
			if (apply)
			{
				apply(std::move(value));
			}

			std::lock_guard lock(mutex_);
			completed_ = true;
			applied_ = true;
		}

		void CompleteException(std::exception_ptr exception)
		{
			std::lock_guard lock(mutex_);
			if (completed_)
			{
				return;
			}
			completed_ = true;
			quarantined_ = true;
			quarantine_->StoreException(std::move(exception));
		}

		bool IsCancelled() const
		{
			std::lock_guard lock(mutex_);
			return cancelled_;
		}

		bool WasApplied() const
		{
			std::lock_guard lock(mutex_);
			return applied_;
		}

		bool WasQuarantined() const
		{
			std::lock_guard lock(mutex_);
			return quarantined_;
		}

	private:
		StopToken token_;
		Quarantine* quarantine_;
		ApplyFn apply_;
		mutable std::mutex mutex_;
		bool started_{ false };
		bool cancelled_{ false };
		bool completed_{ false };
		bool applied_{ false };
		bool quarantined_{ false };
	};

	template <>
	class OperationState<void>
	{
	public:
		using ApplyFn = std::function<void()>;

		OperationState(StopToken token, Quarantine& quarantine, ApplyFn apply = {})
		    : token_(std::move(token))
		    , quarantine_(&quarantine)
		    , apply_(std::move(apply))
		{
		}

		bool TryStart()
		{
			if (token_.StopRequested())
			{
				std::lock_guard lock(mutex_);
				if (!completed_)
				{
					completed_ = true;
					quarantined_ = true;
					RuntimeMetrics::Instance().cancelled_before_start.fetch_add(1, std::memory_order_relaxed);
					quarantine_->StoreCancelled();
				}
				return false;
			}

			std::lock_guard lock(mutex_);
			if (completed_)
			{
				return false;
			}
			if (cancelled_ || token_.StopRequested())
			{
				completed_ = true;
				quarantined_ = true;
				RuntimeMetrics::Instance().cancelled_before_start.fetch_add(1, std::memory_order_relaxed);
				quarantine_->StoreCancelled();
				return false;
			}
			started_ = true;
			return true;
		}

		void Cancel()
		{
			std::lock_guard lock(mutex_);
			cancelled_ = true;
			if (!started_ && !completed_)
			{
				completed_ = true;
				quarantined_ = true;
				RuntimeMetrics::Instance().cancelled_before_start.fetch_add(1, std::memory_order_relaxed);
				quarantine_->StoreCancelled();
			}
		}

		void Complete()
		{
			ApplyFn apply;
			bool quarantine_unit = false;
			{
				std::lock_guard lock(mutex_);
				if (completed_)
				{
					return;
				}
				if (cancelled_ || token_.StopRequested())
				{
					completed_ = true;
					quarantined_ = true;
					quarantine_unit = true;
				}
				else
				{
					apply = apply_;
				}
			}

			if (quarantine_unit)
			{
				RuntimeMetrics::Instance().late_completion_quarantined.fetch_add(1, std::memory_order_relaxed);
				quarantine_->StoreUnit();
				return;
			}
			if (apply)
			{
				apply();
			}

			std::lock_guard lock(mutex_);
			completed_ = true;
			applied_ = true;
		}

		void CompleteException(std::exception_ptr exception)
		{
			std::lock_guard lock(mutex_);
			if (completed_)
			{
				return;
			}
			completed_ = true;
			quarantined_ = true;
			quarantine_->StoreException(std::move(exception));
		}

		bool WasApplied() const
		{
			std::lock_guard lock(mutex_);
			return applied_;
		}

		bool WasQuarantined() const
		{
			std::lock_guard lock(mutex_);
			return quarantined_;
		}

	private:
		StopToken token_;
		Quarantine* quarantine_;
		ApplyFn apply_;
		mutable std::mutex mutex_;
		bool started_{ false };
		bool cancelled_{ false };
		bool completed_{ false };
		bool applied_{ false };
		bool quarantined_{ false };
	};

	class TaskScope
	{
	public:
		explicit TaskScope(IContinuationExecutor& executor);
		TaskScope(IContinuationExecutor& executor, StopToken parent_token);
		TaskScope(IContinuationExecutor& executor, StopSource& parent_source);
		~TaskScope();

		TaskScope(const TaskScope&) = delete;
		TaskScope& operator=(const TaskScope&) = delete;
		TaskScope(TaskScope&&) = delete;
		TaskScope& operator=(TaskScope&&) = delete;

		void RequestStop() noexcept;
		bool StopRequested() const noexcept;
		StopToken GetStopToken() const;
		void Join();
		std::size_t InFlight() const noexcept;
		Quarantine& QuarantineSink() noexcept;
		const Quarantine& QuarantineSink() const noexcept;

		template <typename Fn>
		void Submit(Fn&& fn)
		{
			BeginChild();
			StopToken token = GetStopToken();
			executor_->Submit([this, token = std::move(token), fn = std::forward<Fn>(fn)]() mutable {
				if (token.StopRequested())
				{
					RuntimeMetrics::Instance().cancelled_before_start.fetch_add(1, std::memory_order_relaxed);
					quarantine_->StoreCancelled();
					FinishChild();
					return;
				}

				try
				{
					detail::InvokeWithOptionalToken(fn, token);
				}
				catch (...)
				{
					quarantine_->StoreException(std::current_exception());
				}

				FinishChild();
			});
		}

		template <typename Work, typename Apply>
		auto SubmitOperation(Work&& work, Apply&& apply)
		{
			using Result = detail::InvokeResult<Work>;
			auto state = std::make_shared<OperationState<Result>>(
			    GetStopToken(), *quarantine_, std::forward<Apply>(apply));

			BeginChild();
			StopToken token = GetStopToken();
			executor_->Submit([this, token = std::move(token), state, work = std::forward<Work>(work)]() mutable {
				if (!state->TryStart())
				{
					FinishChild();
					return;
				}

				try
				{
					if constexpr (std::is_void_v<Result>)
					{
						detail::InvokeWithOptionalToken(work, token);
						state->Complete();
					}
					else
					{
						state->Complete(detail::InvokeWithOptionalToken(work, token));
					}
				}
				catch (...)
				{
					state->CompleteException(std::current_exception());
				}

				FinishChild();
			});

			return state;
		}

	private:
		void BeginChild();
		void FinishChild() noexcept;

		IContinuationExecutor* executor_;
		StopSource stop_source_;
		StopToken parent_token_;
		std::shared_ptr<Quarantine> quarantine_;
		std::atomic<std::size_t> in_flight_{ 0 };
		mutable std::mutex join_mutex_;
		std::condition_variable join_cv_;
	};

} // namespace prism

#endif
