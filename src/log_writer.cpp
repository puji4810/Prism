#include "log_writer.h"

#include "crc32.h"
#include "env.h"

#include <cassert>
#include <cstdint>

namespace prism::log
{
	namespace
	{
		void InitTypeCrc(uint32_t* type_crc)
		{
			for (int i = 0; i <= kMaxRecordType; ++i)
			{
				const char t = static_cast<char>(i);
				type_crc[i] = crc32c::Crc32c(&t, 1);
			}
		}
	}

	Writer::Writer(WritableFile* dest)
	    : dest_(dest)
	    , block_offset_(0)
	{
		assert(dest_ != nullptr);
		InitTypeCrc(type_crc_);
	}

	Writer::Writer(WritableFile* dest, uint64_t dest_length)
	    : dest_(dest)
	    , block_offset_(static_cast<int>(dest_length % kBlockSize))
	{
		assert(dest_ != nullptr);
		InitTypeCrc(type_crc_);
	}

	Status Writer::AddRecord(const Slice& record)
	{
		const char* ptr = record.data();
		size_t left = record.size();

		// Fragment the record if necessary and emit it.
		// Note that if record is empty, we still want to iterate once
		// to emit a single zero-length record.
		bool begin = true;
		do
		{
			const int leftover = kBlockSize - block_offset_;
			assert(leftover >= 0);

			if (leftover < kHeaderSize)
			{
				// Switch to a new block.
				if (leftover > 0)
				{
					const char zeros[kHeaderSize] = { 0 };
					Status s = dest_->Append(Slice(zeros, static_cast<size_t>(leftover)));
					if (!s.ok())
					{
						return s;
					}
				}
				block_offset_ = 0;
			}

			assert(kBlockSize - block_offset_ - kHeaderSize >= 0);
			const size_t avail = static_cast<size_t>(kBlockSize - block_offset_ - kHeaderSize);
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

			Status s = EmitPhysicalRecord(type, ptr, fragment_length);
			if (!s.ok())
			{
				return s;
			}

			ptr += fragment_length;
			left -= fragment_length;
			begin = false;
		} while (left > 0);

		return Status::OK();
	}

	Status Writer::EmitPhysicalRecord(RecordType type, const char* ptr, size_t length)
	{
		assert(length <= 0xffff);
		assert(block_offset_ + kHeaderSize + static_cast<int>(length) <= kBlockSize);

		uint32_t crc = crc32c::Extend(type_crc_[static_cast<int>(type)],
		    reinterpret_cast<const uint8_t*>(ptr), length);
		crc = Mask(crc);

		Header header;
		header.checksum = crc;
		header.length = static_cast<uint16_t>(length);
		header.type = type;

		char buf[kHeaderSize];
		header.EncodeTo(buf);

		Status s = dest_->Append(Slice(buf, kHeaderSize));
		if (s.ok())
		{
			s = dest_->Append(Slice(ptr, length));
		}
		if (s.ok())
		{
			block_offset_ += kHeaderSize + static_cast<int>(length);
		}
		return s;
	}
}

