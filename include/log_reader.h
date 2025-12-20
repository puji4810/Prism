#ifndef LOG_READER_H_
#define LOG_READER_H_

#include <cstdint>
#include <string>

#include "slice.h"
#include "status.h"
#include "log_format.h"

namespace prism
{
	class SequentialFile;

	namespace log
	{
		class Reader
		{
		public:
			class Reporter
			{
			public:
				virtual ~Reporter();
				virtual void Corruption(size_t bytes, const Status& status) = 0;
			};

			Reader(SequentialFile* src, Reporter* reporter, bool verify_checksums, uint64_t initial_offset);
			~Reader();

			Reader(const Reader&) = delete;
			Reader& operator=(const Reader&) = delete;
			Reader(Reader&&) = delete;
			Reader& operator=(Reader&&) = delete;

			bool ReadRecord(Slice* record, std::string* scratch);
			[[nodiscard]] Status status() const { return status_; }

		private:
			enum
			{
				kEof = kMaxRecordType + 1,
				kBadRecord = kMaxRecordType + 2
			};

			bool SkipToInitialBlock();

			unsigned int ReadPhysicalRecord(Slice* result);
			void ReportCorruption(uint64_t bytes, const char* reason);
			void ReportDrop(uint64_t bytes, const Status& reason);

			SequentialFile* src_;
			Reporter* reporter_;
			bool checksum_;

			char* const backing_store_;
			Slice buffer_;
			bool eof_;
			uint64_t last_record_offset_;
			uint64_t end_of_buffer_offset_;
			uint64_t initial_offset_;
			bool resyncing_;
			Status status_;
		};
	}
}

#endif // LOG_READER_H__
