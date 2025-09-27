#ifndef LOG_FORMAT_H_
#define LOG_FORMAT_H_

#include <cstdint>
#include "slice.h"
#include "crc32c/crc32c.h"

namespace prism
{
	static const int kBlockSize = 32768; // 32KB block size

	namespace log
	{
		static const int kHeaderSize = 7; // Header is checksum (4 bytes), length (2 bytes), type (1 byte).

		enum class RecordType : unsigned char
		{
			kZeroType = 0,
			kFullType = 1,
			kFirstType = 2,
			kMiddleType = 3,
			kLastType = 4
		};

		struct Header
		{
			uint32_t checksum;
			uint16_t length;
			RecordType type = RecordType::kZeroType;

			Header() = default;

			void EncodeTo(char out[kHeaderSize]) const;
			void DecodeFrom(const char* in);
		};

		inline void Header::EncodeTo(char out[kHeaderSize]) const
		{
			// checksum
			out[0] = static_cast<char>(checksum & 0xff);
			out[1] = static_cast<char>((checksum >> 8) & 0xff);
			out[2] = static_cast<char>((checksum >> 16) & 0xff);
			out[3] = static_cast<char>((checksum >> 24) & 0xff);

			// len
			out[4] = static_cast<char>(length & 0xff);
			out[5] = static_cast<char>((length >> 8) & 0xff);

			// type
			out[6] = static_cast<char>(type);
		}

		inline void Header::DecodeFrom(const char* in)
		{
			// Header h;
			
			checksum = static_cast<uint32_t>(static_cast<unsigned char>(in[0]))
			    | (static_cast<uint32_t>(static_cast<unsigned char>(in[1])) << 8)
			    | (static_cast<uint32_t>(static_cast<unsigned char>(in[2])) << 16)
			    | (static_cast<uint32_t>(static_cast<unsigned char>(in[3])) << 24);

			length = static_cast<uint16_t>(static_cast<unsigned char>(in[4]))
			    | (static_cast<uint16_t>(static_cast<unsigned char>(in[5])) << 8);

			type = static_cast<RecordType>(static_cast<unsigned char>(in[6]));
		}
	}
}

#endif // LOG_FORMAT_H_