#ifndef LOG_READER_H_
#define LOG_READER_H_

#include <string>
#include <fstream>
#include "slice.h"

namespace prism
{
	namespace log
	{
		class Reader
		{
		public:
			explicit Reader(const std::string& src);
			~Reader();

			bool FillBuffer();
			bool ReadRecord(Slice& record);

		private:
			Slice buffer_;
			char* const backing_store_;
			std::ifstream src_;
			bool eof_ = false;
		};
	}
}

#endif // LOG_READER_H__