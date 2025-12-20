#include "log_reader.h"

#include "crc32.h"
#include "env.h"

#include <cstdio>

namespace prism::log
{
	Reader::Reporter::~Reporter() = default;

	Reader::Reader(SequentialFile* src, Reporter* reporter, bool verify_checksums, uint64_t initial_offset)
	    : src_(src)
	    , reporter_(reporter)
	    , checksum_(verify_checksums)
	    , backing_store_(new char[kBlockSize])
	    , buffer_()
	    , eof_(false)
	    , last_record_offset_(0)
	    , end_of_buffer_offset_(0)
	    , initial_offset_(initial_offset)
	    , resyncing_(initial_offset > 0)
	    , status_()
	{
	}

	Reader::~Reader() { delete[] backing_store_; }

	bool Reader::SkipToInitialBlock()
	{
		const size_t offset_in_block = static_cast<size_t>(initial_offset_ % kBlockSize);
		uint64_t block_start_location = initial_offset_ - offset_in_block;

		// Don't search a block if we'd be in the trailer.
		if (offset_in_block > static_cast<size_t>(kBlockSize - kHeaderSize))
		{
			block_start_location += kBlockSize;
		}

		end_of_buffer_offset_ = block_start_location;
		if (block_start_location > 0)
		{
			Status s = src_->Skip(block_start_location);
			if (!s.ok())
			{
				ReportDrop(block_start_location, s);
				return false;
			}
		}

		return true;
	}

	bool Reader::ReadRecord(Slice* record, std::string* scratch)
	{
		if (last_record_offset_ < initial_offset_)
		{
			if (!SkipToInitialBlock())
			{
				return false;
			}
		}

		scratch->clear();
		record->clear();
		bool in_fragmented_record = false;
		uint64_t prospective_record_offset = 0;

		Slice fragment;
		while (true)
		{
			const unsigned int record_type = ReadPhysicalRecord(&fragment);

			const uint64_t physical_record_offset =
			    end_of_buffer_offset_ - fragment.size() - kHeaderSize - buffer_.size();

			if (resyncing_)
			{
				if (record_type == static_cast<unsigned int>(RecordType::kMiddleType))
				{
					continue;
				}
				if (record_type == static_cast<unsigned int>(RecordType::kLastType))
				{
					resyncing_ = false;
					continue;
				}
				resyncing_ = false;
			}

			switch (record_type)
			{
			case static_cast<unsigned int>(RecordType::kFullType):
				if (in_fragmented_record && !scratch->empty())
				{
					ReportCorruption(scratch->size(), "partial record without end(1)");
				}
				prospective_record_offset = physical_record_offset;
				scratch->clear();
				*record = fragment;
				last_record_offset_ = prospective_record_offset;
				return true;

			case static_cast<unsigned int>(RecordType::kFirstType):
				if (in_fragmented_record && !scratch->empty())
				{
					ReportCorruption(scratch->size(), "partial record without end(2)");
				}
				prospective_record_offset = physical_record_offset;
				scratch->assign(fragment.data(), fragment.size());
				in_fragmented_record = true;
				break;

			case static_cast<unsigned int>(RecordType::kMiddleType):
				if (!in_fragmented_record)
				{
					ReportCorruption(fragment.size(), "missing start of fragmented record(1)");
				}
				else
				{
					scratch->append(fragment.data(), fragment.size());
				}
				break;

			case static_cast<unsigned int>(RecordType::kLastType):
				if (!in_fragmented_record)
				{
					ReportCorruption(fragment.size(), "missing start of fragmented record(2)");
				}
				else
				{
					scratch->append(fragment.data(), fragment.size());
					*record = Slice(*scratch);
					last_record_offset_ = prospective_record_offset;
					return true;
				}
				break;

			case kEof:
				if (in_fragmented_record)
				{
					scratch->clear();
				}
				return false;

			case kBadRecord:
				if (in_fragmented_record)
				{
					ReportCorruption(scratch->size(), "error in middle of record");
					in_fragmented_record = false;
					scratch->clear();
				}
				break;

			default:
			{
				char buf[40];
				std::snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
				ReportCorruption(fragment.size() + (in_fragmented_record ? scratch->size() : 0), buf);
				in_fragmented_record = false;
				scratch->clear();
				break;
			}
			}
		}
	}

	void Reader::ReportCorruption(uint64_t bytes, const char* reason)
	{
		ReportDrop(bytes, Status::Corruption(reason));
	}

	void Reader::ReportDrop(uint64_t bytes, const Status& reason)
	{
		if (status_.ok())
		{
			status_ = reason;
		}
		if (reporter_ != nullptr && end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_)
		{
			reporter_->Corruption(static_cast<size_t>(bytes), reason);
		}
	}

	unsigned int Reader::ReadPhysicalRecord(Slice* result)
	{
		while (true)
		{
			if (buffer_.size() < kHeaderSize)
			{
				if (!eof_)
				{
					buffer_.clear();
					Status s = src_->Read(kBlockSize, &buffer_, backing_store_);
					end_of_buffer_offset_ += buffer_.size();
					if (!s.ok())
					{
						buffer_.clear();
						ReportDrop(kBlockSize, s);
						eof_ = true;
						return kEof;
					}
					if (buffer_.size() < static_cast<size_t>(kBlockSize))
					{
						eof_ = true;
					}
					continue;
				}

				buffer_.clear();
				return kEof;
			}

			const char* header_bytes = buffer_.data();
			Header header;
			header.DecodeFrom(header_bytes);
			const size_t length = header.length;
			const unsigned int type = static_cast<unsigned int>(static_cast<unsigned char>(header.type));

			if (kHeaderSize + length > buffer_.size())
			{
				const size_t drop_size = buffer_.size();
				buffer_.clear();
				if (!eof_)
				{
					ReportCorruption(drop_size, "bad record length");
					return kBadRecord;
				}
				return kEof;
			}

			if (type == static_cast<unsigned int>(RecordType::kZeroType) && length == 0)
			{
				buffer_.clear();
				return kBadRecord;
			}

			if (checksum_)
			{
				const uint32_t expected_crc = Unmask(header.checksum);
				const char type_byte = static_cast<char>(header.type);
				uint32_t actual_crc = crc32c::Crc32c(&type_byte, 1);
				actual_crc = crc32c::Extend(actual_crc,
				    reinterpret_cast<const uint8_t*>(header_bytes + kHeaderSize), length);
				if (actual_crc != expected_crc)
				{
					const size_t drop_size = buffer_.size();
					buffer_.clear();
					ReportCorruption(drop_size, "checksum mismatch");
					return kBadRecord;
				}
			}

			buffer_.remove_prefix(kHeaderSize + length);

			if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length < initial_offset_)
			{
				result->clear();
				return kBadRecord;
			}

			*result = Slice(header_bytes + kHeaderSize, length);
			return type;
		}
	}
}

