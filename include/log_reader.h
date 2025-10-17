#ifndef LOG_READER_H_
#define LOG_READER_H_

#include <string>
#include <fstream>
#include "slice.h"
#include "log_format.h"

namespace prism
{
	namespace log
	{
		class Reader
		{
		public:
			explicit Reader(const std::string& src);
			~Reader();

			bool ReadRecord(Slice& record);

		private:
			enum
			{
				kEof = kMaxRecordType + 1,
				kBadRecord = kMaxRecordType + 2
			};

			bool ReadPhysicalRecord(Slice& result, RecordType& type);

			Slice buffer_;
			char* const backing_store_;
			std::ifstream src_;
			bool eof_ = false; // Tag to indicate the end of the file
			std::string scratch_; // Buffer for assembling fragmented records
		};
	}
}

#endif // LOG_READER_H__