#include "db_impl.h"
#include <cassert>

int main()
{
	auto db = prism::DB::Open("my_database");
	db->Put("key2", "value2\n");
	auto value = db->Get("key2");
	if (value)
	{
		printf("Got value: %s\n", value->c_str());
	}
	else
	{
		printf("Key not found\n");
	}
}
