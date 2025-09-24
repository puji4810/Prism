#include "log_reader.h"
#include "slice.h"
#include "log_format.h"
#include <algorithm>
#include <cstring>
#include <iterator>

namespace prism
{
	namespace log
	{
		Reader::Reader(const std::string& src)
		    : src_(src)
		    , backing_store_(new char[kBlockSize])
		{
			if (!src_)
			{
				throw std::runtime_error("Failed to open log file: " + src);
			}
		}

		bool Reader::FillBuffer()
		{
			if (!buffer_.empty())
			{
				return false;
			}
			src_.read(backing_store_, kBlockSize);
			size_t bytes_read = src_.gcount();
			if (bytes_read == 0)
			{
				eof_ = true;
				return false;
			}
			buffer_ = Slice(backing_store_, bytes_read);
			return true;
		}

		bool Reader::ReadRecord(Slice& record)
		{
			while (true)
			{
				if (buffer_.empty() && !FillBuffer())
				{
					return false; // No more data
				}

				auto newline = std::ranges::find_if(buffer_, [](char c) { return c == '\n'; });
				if (newline != buffer_.end())
				{
					size_t record_size = std::distance(buffer_.begin(), newline);
					record = Slice(buffer_.data(), record_size);
					buffer_.remove_prefix(record_size + 1); // +1 to skip the newline
					return true;
				}
				else
				{
					// No newline found, need to read more data
					if (eof_)
					{
						// If we reached EOF and there's no newline, return the remaining data as a record
						if (!buffer_.empty())
						{
							record = buffer_;
							buffer_.clear();
							return true;
						}
						return false; // No more data
					}
					// Move existing data to the start of backing_store_ if necessary
					size_t existing_size = buffer_.size();
					if (existing_size < kBlockSize)
					{
						memmove(backing_store_, buffer_.data(), existing_size);
						buffer_ = Slice(backing_store_, existing_size);
					}
					else
					{
						throw std::runtime_error("Record too large to fit in buffer");
					}
					// Read more data into backing_store_
					src_.read(backing_store_ + existing_size, kBlockSize - existing_size);
					size_t bytes_read = src_.gcount();
					if (bytes_read == 0)
					{
						eof_ = true;
					}
					buffer_ = Slice(backing_store_, existing_size + bytes_read);
				}
			}
		}

		Reader::~Reader()
		{
			if (src_.is_open())
			{
				src_.close();
			}
		}

	}
}