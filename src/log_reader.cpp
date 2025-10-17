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

		bool Reader::ReadRecord(Slice& record)
		{
			scratch_.clear();
			bool in_fragmented_record = false;

			while (true)
			{
				Slice fragment;
				RecordType record_type;

				if (!ReadPhysicalRecord(fragment, record_type))
				{
					// EOF or error
					if (in_fragmented_record)
					{
						// Incomplete fragmented record at end of file
						scratch_.clear();
					}
					return false;
				}

				switch (record_type)
				{
					case RecordType::kFullType:
						if (in_fragmented_record)
						{
							// Handle bug in earlier versions where
							// it could emit an empty kFirstType at tail end
							if (!scratch_.empty())
							{
								throw std::runtime_error("Partial record without end");
							}
						}
						scratch_.clear();
						record = fragment;
						return true;

					case RecordType::kFirstType:
						if (in_fragmented_record)
						{
							// Handle bug in earlier versions
							if (!scratch_.empty())
							{
								throw std::runtime_error("Partial record without end");
							}
						}
						scratch_.assign(fragment.data(), fragment.size());
						in_fragmented_record = true;
						break;

					case RecordType::kMiddleType:
						if (!in_fragmented_record)
						{
							throw std::runtime_error("Missing start of fragmented record");
						}
						scratch_.append(fragment.data(), fragment.size());
						break;

					case RecordType::kLastType:
						if (!in_fragmented_record)
						{
							throw std::runtime_error("Missing start of fragmented record");
						}
						scratch_.append(fragment.data(), fragment.size());
						record = Slice(scratch_);
						return true;

					default:
						throw std::runtime_error("Unknown record type");
				}
			}
		}

		bool Reader::ReadPhysicalRecord(Slice& result, RecordType& type)
		{
			while (true)
			{
				// Need to read more data
				if (buffer_.size() < kHeaderSize)
				{
					if (!eof_)
					{
						// Last read was a full read, clear buffer and read next block
						// Compare with clear the buffer, use memove to move the unused data to the beginning of the buffer
						// clear the buffer may get better performance for small blocks
						buffer_.clear();
						src_.read(backing_store_, kBlockSize);
						size_t n = src_.gcount();
						
						if (n == 0) // finish reading
						{
							eof_ = true;
							buffer_.clear();
							return false;
						}
						else if (n < kBlockSize)
						{
							eof_ = true;
						}
						
						buffer_ = Slice(backing_store_, n);
						continue;
					}
					else
					{
						// Truncated header at end of file
						buffer_.clear();
						return false;
					}
				}

				// Parse the header using Header struct
				Header header;
				header.DecodeFrom(buffer_.data());

				// Check if we have enough data for the payload
				if (kHeaderSize + header.length > buffer_.size())
				{
					if (!eof_)
					{
						throw std::runtime_error("Bad record length");
					}
					// Truncated record at end of file
					buffer_.clear();
					return false;
				}

				// Skip zero-length records (padding)
				if (header.type == RecordType::kZeroType && header.length == 0)
				{
					buffer_.remove_prefix(kHeaderSize);
					continue;
				}

				// Verify checksum
				const char type_byte = static_cast<char>(header.type);
				uint32_t expected_crc = Unmask(header.checksum);

				uint32_t actual_crc = crc32c::Crc32c(reinterpret_cast<const uint8_t*>(&type_byte), 1);
				actual_crc = crc32c::Extend(actual_crc,
				                            reinterpret_cast<const uint8_t*>(buffer_.data() + kHeaderSize),
				                            header.length);

				if (actual_crc != expected_crc)
				{
					throw std::runtime_error("Checksum mismatch in log record");
				}

				// Return the record
				result = Slice(buffer_.data() + kHeaderSize, header.length);
				buffer_.remove_prefix(kHeaderSize + header.length);
				type = header.type;
				return true;
			}
		}

		Reader::~Reader()
		{
			if (src_.is_open())
			{
				src_.close();
			}
			delete[] backing_store_;
		}

	}
}