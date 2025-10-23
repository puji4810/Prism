#ifndef PRISM_DB_H
#define PRISM_DB_H

#include <string>
#include <memory>
#include "status.h"
#include "slice.h"
#include "write_batch.h"

namespace prism
{
	class DB
	{
	public:
		DB() = default;
		virtual ~DB();
		
		// Open the database with the specified "name".
		// Returns a pointer to a heap-allocated database on success.
		// Returns nullptr on error.
		static std::unique_ptr<DB> Open(const std::string& dbname);
		
		// Set the database entry for "key" to "value".
		// Returns OK on success, and a non-OK status on error.
		virtual Status Put(const Slice& key, const Slice& value) = 0;
		
		// If the database contains an entry for "key" store the
		// corresponding value in *value and return OK.
		//
		// If there is no entry for "key" leave *value unchanged and return
		// a status for which Status::IsNotFound() returns true.
		//
		// May return some other Status on an error.
		virtual Status Get(const Slice& key, std::string* value) = 0;
		
		// Remove the database entry (if any) for "key".  Returns OK on
		// success, and a non-OK status on error.  It is not an error if "key"
		// did not exist in the database.
		virtual Status Delete(const Slice& key) = 0;
		
		// Apply the specified updates to the database.
		// Returns OK on success, non-OK on failure.
		virtual Status Write(WriteBatch& batch) = 0;
	};
} // namespace prism

#endif // PRISM_DB_H