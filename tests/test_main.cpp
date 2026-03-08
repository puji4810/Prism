// test_main.cpp – shared GoogleTest main() for test binaries that don't
// define their own.  Include this file in the xmake target instead of linking
// gtest_main so that we can add custom initialization here if needed later.

#include <gtest/gtest.h>

int main(int argc, char** argv)
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
