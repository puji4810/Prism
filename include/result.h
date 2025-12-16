#ifndef PRISM_RESULT_H
#define PRISM_RESULT_H

#include "status.h"

#include <cassert>
#include <type_traits>
#include <utility>

namespace prism
{
	namespace detail
	{
		inline const Status& OkStatus() noexcept
		{
			static const Status ok;
			return ok;
		}
	}

	template <typename T>
	class Result
	{
	public:
		Result(const T& value)
		    : has_value_(true)
		{
			new (&storage_.value_) T(value);
		}

		Result(T&& value)
		    : has_value_(true)
		{
			new (&storage_.value_) T(std::move(value));
		}

		Result(const Status& status)
		    : has_value_(false)
		{
			assert(!status.ok());
			new (&storage_.status_) Status(status);
		}

		Result(Status&& status)
		    : has_value_(false)
		{
			assert(!status.ok());
			new (&storage_.status_) Status(std::move(status));
		}

		Result(const Result& other) requires(std::is_copy_constructible_v<T>)
		    : has_value_(other.has_value_)
		{
			if (has_value_)
			{
				new (&storage_.value_) T(other.storage_.value_);
			}
			else
			{
				new (&storage_.status_) Status(other.storage_.status_);
			}
		}

		Result(Result&& other) noexcept(std::is_nothrow_move_constructible_v<T>)
		    : has_value_(other.has_value_)
		{
			if (has_value_)
			{
				new (&storage_.value_) T(std::move(other.storage_.value_));
			}
			else
			{
				new (&storage_.status_) Status(std::move(other.storage_.status_));
			}
		}

		~Result() { Destroy(); }

		Result& operator=(const Result& other) requires(std::is_copy_constructible_v<T>&& std::is_copy_assignable_v<T>)
		{
			if (this == &other)
			{
				return *this;
			}

			if (has_value_ && other.has_value_)
			{
				storage_.value_ = other.storage_.value_;
				return *this;
			}

			if (!has_value_ && !other.has_value_)
			{
				storage_.status_ = other.storage_.status_;
				return *this;
			}

			Destroy();
			has_value_ = other.has_value_;
			if (has_value_)
			{
				new (&storage_.value_) T(other.storage_.value_);
			}
			else
			{
				new (&storage_.status_) Status(other.storage_.status_);
			}
			return *this;
		}

		Result& operator=(Result&& other) noexcept(std::is_nothrow_move_constructible_v<T>&& std::is_nothrow_move_assignable_v<T>)
		{
			if (this == &other)
			{
				return *this;
			}

			if (has_value_ && other.has_value_)
			{
				storage_.value_ = std::move(other.storage_.value_);
				return *this;
			}

			if (!has_value_ && !other.has_value_)
			{
				storage_.status_ = std::move(other.storage_.status_);
				return *this;
			}

			Destroy();
			has_value_ = other.has_value_;
			if (has_value_)
			{
				new (&storage_.value_) T(std::move(other.storage_.value_));
			}
			else
			{
				new (&storage_.status_) Status(std::move(other.storage_.status_));
			}
			return *this;
		}

		[[nodiscard]] bool ok() const noexcept { return has_value_; }
		explicit operator bool() const noexcept { return ok(); }

		[[nodiscard]] const Status& status() const noexcept
		{
			return has_value_ ? detail::OkStatus() : storage_.status_;
		}

		T& value() &
		{
			assert(has_value_);
			return storage_.value_;
		}

		const T& value() const&
		{
			assert(has_value_);
			return storage_.value_;
		}

		T&& value() &&
		{
			assert(has_value_);
			return std::move(storage_.value_);
		}

		T* operator->() { return &value(); }
		const T* operator->() const { return &value(); }

		T& operator*() & { return value(); }
		const T& operator*() const& { return value(); }
		T&& operator*() && { return std::move(*this).value(); }

	private:
		union Storage
		{
			Storage() {}
			~Storage() {}

			T value_;
			Status status_;
		};

		void Destroy() noexcept
		{
			if (has_value_)
			{
				storage_.value_.~T();
			}
			else
			{
				storage_.status_.~Status();
			}
		}

		Storage storage_;
		bool has_value_;
	};

	template <>
	class Result<void>
	{
	public:
		Result()
		    : status_(Status::OK())
		{
		}

		Result(const Status& status)
		    : status_(status)
		{
		}

		Result(Status&& status)
		    : status_(std::move(status))
		{
		}

		[[nodiscard]] bool ok() const noexcept { return status_.ok(); }
		explicit operator bool() const noexcept { return ok(); }

		[[nodiscard]] const Status& status() const noexcept { return status_; }

	private:
		Status status_;
	};
} // namespace prism

#endif // PRISM_RESULT_H
