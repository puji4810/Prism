// TODO 

// #include <cassert>
// #include <utility>
// #include "status.h"

// namespace prism
// {
// 	template <typename T>
// 	class StatusOr
// 	{
// 	public:
// 		StatusOr(T value)
// 		    : status_(Status::OK())
// 		    , value_(std::move(value))
// 		{
// 		}

// 		StatusOr(Status status)
// 		    : status_(std::move(status))
// 		{
// 			assert(!status_.ok());
// 		}

// 		bool ok() const { return status_.ok(); }

// 		const Status& status() const { return status_; }

// 		const T& value() const&
// 		{
// 			assert(ok());
// 			return value_;
// 		}

// 		T value() &&
// 		{
// 			assert(ok());
// 			return std::move(value_);
// 		}

// 	private:
// 		// TODO: optimize by std::variant
// 		Status status_;
// 		T value_;
// 	};
// }
