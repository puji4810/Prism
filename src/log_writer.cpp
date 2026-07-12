#include "log_writer.h"

#include "crc32.h"
#include "env.h"

#include <cassert>
#include <cstdint>

namespace prism::log
{
	namespace
	{
		uint32_t TypeCrc(RecordType type)
		{
			const char t = static_cast<char>(type);
			return crc32c::Crc32c(&t, 1);
		}

		void AppendPhysicalRecord(std::string* dst, RecordType type, const char* ptr, size_t length)
		{
			assert(dst != nullptr);
			assert(length <= 0xffff);

			uint32_t crc = crc32c::Extend(TypeCrc(type), reinterpret_cast<const uint8_t*>(ptr), length);
			crc = Mask(crc);

			Header header;
			header.checksum = crc;
			header.length = static_cast<uint16_t>(length);
			header.type = type;

			char buf[kHeaderSize];
			header.EncodeTo(buf);
			dst->append(buf, kHeaderSize);
			dst->append(ptr, length);
		}
	}

	Result<EncodedRecord> EncodeRecordFragments(const Slice& record, int starting_block_offset)
	{
		if (starting_block_offset < 0 || starting_block_offset >= kBlockSize)
		{
			return std::unexpected(Status::InvalidArgument("invalid WAL block offset"));
		}

		EncodedRecord encoded;
		int block_offset = starting_block_offset;
		const char* ptr = record.data();
		size_t left = record.size();

		bool begin = true;
		do
		{
			const int leftover = kBlockSize - block_offset;
			assert(leftover >= 0);

			if (leftover < kHeaderSize)
			{
				if (leftover > 0)
				{
					const char zeros[kHeaderSize] = { 0 };
					encoded.bytes.append(zeros, static_cast<size_t>(leftover));
				}
				block_offset = 0;
			}

			assert(kBlockSize - block_offset - kHeaderSize >= 0);
			const size_t avail = static_cast<size_t>(kBlockSize - block_offset - kHeaderSize);
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

			AppendPhysicalRecord(&encoded.bytes, type, ptr, fragment_length);
			++encoded.physical_records;
			block_offset += kHeaderSize + static_cast<int>(fragment_length);

			ptr += fragment_length;
			left -= fragment_length;
			begin = false;
		} while (left > 0);

		encoded.ending_block_offset = (block_offset == kBlockSize) ? 0 : block_offset;
		return encoded;
	}

	Writer::Writer(WritableFile* dest)
	    : dest_(dest)
	    , block_offset_(0)
	{
		assert(dest_ != nullptr);
	}

	Writer::Writer(WritableFile* dest, uint64_t dest_length)
	    : dest_(dest)
	    , block_offset_(static_cast<int>(dest_length % kBlockSize))
	{
		assert(dest_ != nullptr);
	}

	Status Writer::AddRecord(const Slice& record)
	{
		auto encoded = EncodeRecordFragments(record, block_offset_);
		if (!encoded.has_value())
		{
			return encoded.error();
		}
		Status s = dest_->Append(Slice(encoded->bytes));
		if (s.ok())
		{
			block_offset_ = encoded->ending_block_offset;
		}
		return s;
	}

	Result<EncodedRecord> Writer::ReserveRecord(const Slice& record)
	{
		auto encoded = EncodeRecordFragments(record, block_offset_);
		if (!encoded.has_value())
		{
			return encoded;
		}
		block_offset_ = encoded->ending_block_offset;
		return encoded;
	}
}
