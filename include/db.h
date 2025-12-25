#ifndef PRISM_DB_H
#define PRISM_DB_H

#include <expected>
#include <string>
#include <memory>
#include "options.h"
#include "status.h"
#include "result.h"
#include "slice.h"
#include "iterator.h"
#include "write_batch.h"

namespace prism
{
	class DB
	{
		// TODO : multi-thread-safe interface
	public:
		DB() = default;
		virtual ~DB();

		// Open the database with the specified "name".
		// Returns a pointer to a heap-allocated database on success.
		// Returns a Result
		static Result<std::unique_ptr<DB>> Open(const Options& opts, const std::string& dbname);
		static Result<std::unique_ptr<DB>> Open(const std::string& dbname);

		// Set the database entry for "key" to "value".
		// Returns OK on success, and a non-OK status on error.
		virtual Status Put(const WriteOptions& options, const Slice& key, const Slice& value) = 0;

		// For all this kinds of api, we just wrap the Func(Options, Args...) by default options
		Status Put(const Slice& key, const Slice& value) { return Put(WriteOptions(), key, value); }

		// If the database contains an entry for "key" store the
		// corresponding value in *value and return OK.
		//
		// If there is no entry for "key" leave *value unchanged and return
		// a status for which Status::IsNotFound() returns true.
		//
		// May return some other Status on an error.
		virtual Result<std::string> Get(const ReadOptions& options, const Slice& key) = 0;
		Result<std::string> Get(const Slice& key) { return Get(ReadOptions(), key); }

		// Remove the database entry (if any) for "key".  Returns OK on
		// success, and a non-OK status on error.  It is not an error if "key"
		// did not exist in the database.
		virtual Status Delete(const WriteOptions& options, const Slice& key) = 0;
		Status Delete(const Slice& key) { return Delete(WriteOptions(), key); }

		// Apply the specified updates to the database.
		// Returns OK on success, non-OK on failure.
		virtual Status Write(const WriteOptions& options, WriteBatch batch) = 0;
		Status Write(WriteBatch batch) { return Write(WriteOptions(), std::move(batch)); }

		// Return a heap-allocated iterator over the contents of the database.
		// The caller must delete the iterator when it is no longer needed.
		virtual Iterator* NewIterator(const ReadOptions& options) = 0;

		virtual const Snapshot* GetSnapshot() = 0;
		virtual void ReleaseSnapshot(const Snapshot* snapshot) = 0;
	};

	Status DestroyDB(const std::string& dbname, const Options& options);
} // namespace prism

#endif // PRISM_DB_H
