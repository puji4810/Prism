#ifndef PRISM_ASYNC_WAL_WRITER_H
#define PRISM_ASYNC_WAL_WRITER_H

#include "slice.h"
#include "status.h"

#include <cstdint>
#include <functional>

namespace prism
{
	class AsyncRuntime;
	class WritableFile;

	namespace log
	{
		class Writer;
	}

	class AsyncWalWriter final
	{
	public:
		using Completion = std::function<void(Status)>;

		explicit AsyncWalWriter(AsyncRuntime& runtime);

		AsyncWalWriter(const AsyncWalWriter&) = delete;
		AsyncWalWriter& operator=(const AsyncWalWriter&) = delete;
		AsyncWalWriter(AsyncWalWriter&&) = delete;
		AsyncWalWriter& operator=(AsyncWalWriter&&) = delete;

		void Write(WritableFile& file, log::Writer& writer, const Slice& record, bool sync, Completion completion);

	private:
		AsyncRuntime* runtime_;
		std::uint64_t next_user_data_ = 1;
	};
} // namespace prism

#endif // PRISM_ASYNC_WAL_WRITER_H
