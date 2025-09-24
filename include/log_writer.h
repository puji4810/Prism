#ifndef LOG_WRITER_H_
#define LOG_WRITER_H_

#include <string>
#include <fstream>
#include "slice.h"

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
		};
	} // namespace log
}

#endif // LOG_WRITER_H_