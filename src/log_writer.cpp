#include "log_writer.h"
#include "crc32c/crc32c.h"
#include "log_format.h"
#include <cstdint>

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
		{
			dest_.open(dest, std::ios::binary | std::ios::app);
			if (!dest_)
			{
				throw std::runtime_error("Failed to open log file: " + dest);
			}
			InitTypeCrc(type_crc_);
		}

		void Writer::AddRecord(const Slice& record)
		{
			Header header;
			header.type = RecordType::kFullType;
			header.length = record.size();

			// checksum = crc32c(type + record)
			uint32_t crc = type_crc_[static_cast<int>(header.type)];
			crc = crc32c::Extend(crc, reinterpret_cast<const uint8_t*>(record.data()), record.size());
			header.checksum = Mask(crc);

			char header_buffer_[kHeaderSize];
			header.EncodeTo(header_buffer_);

			dest_.write(header_buffer_, kHeaderSize);
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