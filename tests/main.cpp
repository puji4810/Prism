#include "db.h"
#include <cstdio>

int main()
{
	auto db_result = prism::DB::Open("my_database");
	if (!db_result.has_value())
	{
		printf("Open failed: %s\n", db_result.error().ToString().c_str());
		return 1;
	}

	auto db = std::move(db_result.value());

	// Example: Put a value
	// prism::Status s = db->Put("key2", "value2");
	// if (!s.ok()) {
	//     printf("Put failed: %s\n", s.ToString().c_str());
	// }

	// Example: Get a value
	auto value_result = db->Get("key2");
	if (value_result.has_value())
	{
		printf("Got value: %s\n", value_result.value().c_str());
	}
	else if (value_result.error().IsNotFound())
	{
		printf("Key not found\n");
	}
	else
	{
		printf("Get failed: %s\n", value_result.error().ToString().c_str());
	}

	return 0;
}
