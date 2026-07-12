#include "env.h"
#include "log_format.h"
#include "log_writer.h"

#include "gtest/gtest.h"

#include <cstddef>
#include <string>

using namespace prism;

namespace
{
	class StringWritableFile final: public WritableFile
	{
	public:
		Status Append(const Slice& data) override
		{
			contents_.append(data.data(), data.size());
			return Status::OK();
		}

		Status Close() override { return Status::OK(); }
		Status Flush() override { return Status::OK(); }
		Status Sync() override { return Status::OK(); }

		const std::string& contents() const noexcept { return contents_; }

	private:
		std::string contents_;
	};

	void ExpectEncoderMatchesWriter(const std::string& record, int starting_block_offset)
	{
		StringWritableFile file;
		log::Writer writer(&file, static_cast<uint64_t>(starting_block_offset));
		ASSERT_TRUE(writer.AddRecord(Slice(record)).ok());

		auto encoded = log::EncodeRecordFragments(Slice(record), starting_block_offset);
		ASSERT_TRUE(encoded.has_value()) << encoded.error().ToString();
		EXPECT_EQ(encoded->bytes, file.contents());
	}
}

TEST(LogWriterTest, SharedEncoderMatchesWriterForEmptyRecord)
{
	ExpectEncoderMatchesWriter("", 0);
}

TEST(LogWriterTest, SharedEncoderMatchesWriterForSmallFullRecord)
{
	ExpectEncoderMatchesWriter("small wal record", 0);
}

TEST(LogWriterTest, SharedEncoderMatchesWriterForRecordEndingAtBlockBoundary)
{
	ExpectEncoderMatchesWriter(std::string(kBlockSize - log::kHeaderSize, 'b'), 0);
}

TEST(LogWriterTest, RecordAfterExactBlockBoundaryStartsNextBlock)
{
	StringWritableFile file;
	log::Writer writer(&file);

	ASSERT_TRUE(writer.AddRecord(Slice(std::string(kBlockSize - log::kHeaderSize, 'b'))).ok());
	ASSERT_TRUE(writer.AddRecord(Slice("next")).ok());

	auto first = log::EncodeRecordFragments(Slice(std::string(kBlockSize - log::kHeaderSize, 'b')), 0);
	ASSERT_TRUE(first.has_value()) << first.error().ToString();
	EXPECT_EQ(0, first->ending_block_offset);

	auto second = log::EncodeRecordFragments(Slice("next"), 0);
	ASSERT_TRUE(second.has_value()) << second.error().ToString();
	EXPECT_EQ(first->bytes + second->bytes, file.contents());
}

TEST(LogWriterTest, SharedEncoderMatchesWriterForFragmentedRecord)
{
	ExpectEncoderMatchesWriter(std::string(kBlockSize * 2 + 123, 'f'), 0);
}

TEST(LogWriterTest, SharedEncoderMatchesWriterForShortBlockTail)
{
	ExpectEncoderMatchesWriter("tail rollover", kBlockSize - 3);
}
