#include "log_reader.h"
#include "slice.h"
#include "log_format.h"
#include <cstring>
#include "crc32c/crc32c.h"

namespace prism
{
	namespace log
	{
		static inline uint32_t Unmask(uint32_t masked)
		{
			uint32_t rot = masked - 0xa282ead8u;
			return (rot << 15) | (rot >> 17);
		}

		Reader::Reader(const std::string& src)
		    : src_(src, std::ios::binary)
		    , backing_store_(new char[kBlockSize])
		{
			if (!src_)
			{
				throw std::runtime_error("Failed to open log file: " + src);
			}
		}

		bool Reader::FillBuffer()
		{
			if (eof_)
				return false;
			size_t existing = buffer_.size();
			std::memmove(backing_store_, buffer_.data(), existing); // move existing data to the beginning of the buffer

			src_.read(backing_store_ + existing, kBlockSize - existing); // always read full block
			size_t n = src_.gcount();
			eof_ = src_.eof();

			if (n == 0)
			{
				return false;
			}
			buffer_ = Slice(backing_store_, existing + n);
			return true;
		}

		bool Reader::ReadRecord(Slice& record)
		{
			for (;;)
			{
				// make sure we have enough header
				while (buffer_.size() < kHeaderSize)
				{
					if (!FillBuffer())
						return false;
				}

				Header h;
				h.DecodeFrom(buffer_.data());

				// ensure full payload in buffer
				while (buffer_.size() < static_cast<size_t>(kHeaderSize + h.length))
				{
					if (eof_)
						return false;
					size_t existing = buffer_.size();
					if (existing >= kBlockSize)
						throw std::runtime_error("Record too large to fit in buffer");
					std::memmove(backing_store_, buffer_.data(), existing);
					src_.read(backing_store_ + existing, kBlockSize - existing);
					size_t n = src_.gcount();
					if (n == 0)
						eof_ = true;
					buffer_ = Slice(backing_store_, existing + n);
				}

				// only support full record (kFullType) until now
				// TODO: support other types
				if (h.type != RecordType::kFullType)
				{
					throw std::runtime_error("Unsupported fragmented record type");
				}

				const char type_byte = static_cast<char>(h.type);
				uint32_t expected = Unmask(h.checksum);
				uint32_t actual = crc32c::Crc32c(reinterpret_cast<const uint8_t*>(&type_byte), 1);
				actual = crc32c::Extend(actual, reinterpret_cast<const uint8_t*>(buffer_.data() + kHeaderSize), h.length);

				if (actual != expected)
				{
					throw std::runtime_error("log corruption: bad record checksum");
				}

				record = Slice(buffer_.data() + kHeaderSize, h.length);
				buffer_.remove_prefix(kHeaderSize + h.length);
				return true;
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