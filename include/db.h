#ifndef PRISM_DB_H
#define PRISM_DB_H

#include <memory>
#include <string>
#include "options.h"
#include "status.h"
#include "result.h"
#include "slice.h"
#include "iterator.h"
#include "write_batch.h"

namespace prism
{
	class AsyncDB;
	class DBImpl;

	// Database: move-only public handle for the KV store.
	//
	// This handle owns the concrete engine directly and is the shipped sync
	// entry point for open/CRUD/iterator/snapshot behavior.
	//
	// Public API surface summary:
	// - Database: synchronous by-value handle.
	// - AsyncDB: coroutine-friendly wrapper for the same engine.
	// - Snapshot: cheap-copy RAII handle captured from Database and passed
	//   back through the snapshot_handle field on ReadOptions.
	class Database
	{
	public:
		Database(const Database&) = delete;
		Database& operator=(const Database&) = delete;
		Database(Database&& other) noexcept;
		Database& operator=(Database&& other) noexcept;
		~Database();

		// Opens the database and returns a by-value Database handle.
		// Use AsyncDB::OpenAsync(...) for coroutine-based callers.
		static Result<Database> Open(const Options& options, const std::string& dbname);
		static Result<Database> Open(const std::string& dbname);

		Status Put(const WriteOptions& options, const Slice& key, const Slice& value);
		Status Put(const Slice& key, const Slice& value) { return Put(WriteOptions(), key, value); }

		Result<std::string> Get(const ReadOptions& options, const Slice& key);
		Result<std::string> Get(const Slice& key) { return Get(ReadOptions(), key); }

		Status Delete(const WriteOptions& options, const Slice& key);
		Status Delete(const Slice& key) { return Delete(WriteOptions(), key); }

		Status Write(const WriteOptions& options, WriteBatch batch);
		Status Write(WriteBatch batch) { return Write(WriteOptions(), std::move(batch)); }

		std::unique_ptr<Iterator> NewIterator(const ReadOptions& options);

		// Captures a cheap-copy Snapshot RAII handle for point-in-time reads.
		// Store it in the snapshot_handle field on ReadOptions when issuing snapshot reads.
		Snapshot CaptureSnapshot();

	private:
		explicit Database(std::unique_ptr<DBImpl> impl);

		std::unique_ptr<DBImpl> impl_;
	};

	Status DestroyDB(const std::string& dbname, const Options& options);
} // namespace prism

#endif // PRISM_DB_H
