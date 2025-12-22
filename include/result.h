#ifndef PRISM_RESULT_H
#define PRISM_RESULT_H

#include "status.h"

#include <expected>

namespace prism
{
	template <typename T>
	using Result = std::expected<T, Status>;
} // namespace prism

#endif // PRISM_RESULT_H
