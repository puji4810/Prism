#ifndef LOG_WRITER_H_
#define LOG_WRITER_H_

#include <cstdint>
#include "slice.h"
#include "status.h"
#include "log_format.h"

namespace prism
{
	class WritableFile;

	namespace log
	{
		class Writer
		{
		public:
			explicit Writer(WritableFile* dest);
			Writer(WritableFile* dest, uint64_t dest_length);

			Writer(const Writer&) = delete;
			Writer& operator=(const Writer&) = delete;
			Writer(Writer&&) = delete;
			Writer& operator=(Writer&&) = delete;

			Status AddRecord(const Slice& record);

		private:
			Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

			WritableFile* dest_;
			uint32_t type_crc_[kMaxRecordType + 1]; // crc32c values for all supported record types.
			int block_offset_;  // Current offset in block
		};
	} // namespace log
}

#endif // LOG_WRITER_H_
