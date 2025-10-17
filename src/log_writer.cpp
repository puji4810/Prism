#include "log_writer.h"
#include "crc32c/crc32c.h"
#include "log_format.h"
#include <cstdint>
#include <cassert>

namespace prism
{
	namespace log
	{
		static void InitTypeCrc(uint32_t* type_crc)
		{
			for (int i = 0; i <= kMaxRecordType; i++)
			{
				char t = static_cast<char>(i);
				type_crc[i] = crc32c::Crc32c(&t, 1);
			}
		}

		static inline uint32_t Mask(uint32_t crc) { return ((crc >> 15) | (crc << 17)) + 0xa282ead8u; }

		Writer::Writer(std::string dest)
		    : block_offset_(0)
		{
			dest_.open(dest, std::ios::binary | std::ios::app);
			if (!dest_)
			{
				throw std::runtime_error("Failed to open log file: " + dest);
			}
			InitTypeCrc(type_crc_);
		}

		Writer::Writer(std::string dest, uint64_t dest_length)
		    : block_offset_(dest_length % kBlockSize)
		{
			dest_.open(dest, std::ios::binary | std::ios::app);
			if (!dest_)
			{
				throw std::runtime_error("Failed to open log file: " + dest);
			}
			InitTypeCrc(type_crc_);
		}

		void Writer::AddRecord(const Slice& slice)
		{
			const char* ptr = slice.data();
			size_t left = slice.size();

			// Fragment the record if necessary and emit it.
			// Note that if slice is empty, we still want to iterate once
			// to emit a single zero-length record
			bool begin = true;
			do
			{
				const int leftover = kBlockSize - block_offset_;
				assert(leftover >= 0);
				
				if (leftover < kHeaderSize)
				{
					// Switch to a new block
					if (leftover > 0)
					{
						// Fill the trailer with zeros
						static_assert(kHeaderSize == 7, "");
						const char zeros[6] = {0, 0, 0, 0, 0, 0};
						dest_.write(zeros, leftover);
					}
					block_offset_ = 0; // A new block is started
				}

				// Invariant: we never leave < kHeaderSize bytes in a block
				assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

				const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
				const size_t fragment_length = (left < avail) ? left : avail;

				RecordType type;
				const bool end = (left == fragment_length);
				if (begin && end)
				{
					type = RecordType::kFullType;
				}
				else if (begin)
				{
					type = RecordType::kFirstType;
				}
				else if (end)
				{
					type = RecordType::kLastType;
				}
				else
				{
					type = RecordType::kMiddleType;
				}

				EmitPhysicalRecord(type, ptr, fragment_length);
				ptr += fragment_length;
				left -= fragment_length;
				begin = false;
			} while (left > 0);
		}

		void Writer::EmitPhysicalRecord(RecordType type, const char* ptr, size_t length)
		{
			assert(length <= 0xffff); // Must fit in two bytes
			assert(block_offset_ + kHeaderSize + length <= kBlockSize);

			// Compute the crc of the record type and the payload
			uint32_t crc = crc32c::Extend(type_crc_[static_cast<int>(type)],
			                               reinterpret_cast<const uint8_t*>(ptr), length);
			crc = Mask(crc); // Adjust for storage

			// Format the header using Header struct
			Header header;
			header.checksum = crc;
			header.length = static_cast<uint16_t>(length);
			header.type = type;

			char buf[kHeaderSize];
			header.EncodeTo(buf);

			// Write the header and the payload
			dest_.write(buf, kHeaderSize);
			dest_.write(ptr, length);
			dest_.flush();
			
			if (!dest_)
			{
				throw std::runtime_error("Failed to write to log file");
			}

			block_offset_ += kHeaderSize + length;
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