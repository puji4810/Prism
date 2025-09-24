#include "log_writer.h"

namespace prism
{
	namespace log
	{
		Writer::Writer(std::string dest)
		{
			dest_.open(dest, std::ios::app);
			if (!dest_)
			{
				throw std::runtime_error("Failed to open log file: " + dest);
			}
		}

		void Writer::AddRecord(const Slice& record)
		{
			dest_.write(record.data(), record.size());
			if (!dest_)
			{
				throw std::runtime_error("Failed to write to log file");
			}
		}

		Writer::~Writer()
		{
			if (dest_.is_open())
			{
				dest_.close();
			}
		}
	} // namespace log

}