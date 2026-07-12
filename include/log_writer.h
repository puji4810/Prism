#ifndef LOG_WRITER_H_
#define LOG_WRITER_H_

#include <cstdint>
#include <string>
#include "slice.h"
#include "status.h"
#include "result.h"
#include "log_format.h"

namespace prism
{
	class WritableFile;

	namespace log
	{
		struct EncodedRecord
		{
			std::string bytes;
			int ending_block_offset = 0;
			std::size_t physical_records = 0;
		};

		Result<EncodedRecord> EncodeRecordFragments(const Slice& record, int starting_block_offset);

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
			Result<EncodedRecord> ReserveRecord(const Slice& record);

		private:
			WritableFile* dest_;
			int block_offset_;  // Current offset in block
		};
	} // namespace log
}

#endif // LOG_WRITER_H_
