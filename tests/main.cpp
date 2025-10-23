#include "db_impl.h"
#include <cstdio>

int main()
{
	auto db = prism::DB::Open("my_database");
	
	// Example: Put a value
	// prism::Status s = db->Put("key2", "value2");
	// if (!s.ok()) {
	//     printf("Put failed: %s\n", s.ToString().c_str());
	// }
	
	// Example: Get a value
	std::string value;
	prism::Status s = db->Get("key2", &value);
	if (s.ok())
	{
		printf("Got value: %s\n", value.c_str());
	}
	else if (s.IsNotFound())
	{
		printf("Key not found\n");
	}
	else
	{
		printf("Get failed: %s\n", s.ToString().c_str());
	}
	
	return 0;
}
