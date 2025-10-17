#ifndef LOG_WRITER_H_
#define LOG_WRITER_H_

#include <string>
#include <fstream>
#include "slice.h"
#include "log_format.h"

namespace prism
{
	namespace log
	{
		class Writer
		{
		public:
			explicit Writer(std::string dest);
			Writer(std::string dest, uint64_t dest_length);

			void AddRecord(const Slice& record);

			~Writer();

		private:
			void EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

			std::ofstream dest_;
			uint32_t type_crc_[kMaxRecordType + 1]; // crc32c values for all supported record types.
			int block_offset_;  // Current offset in block
		};
	} // namespace log
}

#endif // LOG_WRITER_H_