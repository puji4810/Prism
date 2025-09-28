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

			void AddRecord(const Slice& record);

			~Writer();

		private:
			std::ofstream dest_;
			uint32_t type_crc_[kMaxRecordType + 1]; // crc32c values for all supported record types.
		};
	} // namespace log
}

#endif // LOG_WRITER_H_