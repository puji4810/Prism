#include "gtest/gtest.h"

#include "result.h"

#include <memory>
#include <string>

using namespace prism;

namespace
{
	Result<int> OkInt() { return 123; }

	Result<int> ErrInt() { return std::unexpected(Status::NotFound("missing")); }
}

TEST(ResultTest, Value)
{
	Result<int> r = OkInt();
	EXPECT_TRUE(r.has_value());
	EXPECT_EQ(r.value(), 123);
}

TEST(ResultTest, Error)
{
	Result<int> r = ErrInt();
	EXPECT_FALSE(r.has_value());
	EXPECT_TRUE(r.error().IsNotFound());
}

TEST(ResultTest, CopyAndMove)
{
	Result<std::string> a(std::string("hello"));
	Result<std::string> b(a);
	EXPECT_TRUE(b.has_value());
	EXPECT_EQ(b.value(), "hello");

	Result<std::string> c(std::move(b));
	EXPECT_TRUE(c.has_value());
	EXPECT_EQ(c.value(), "hello");

	Result<std::string> d(std::string("x"));
	d = a;
	EXPECT_TRUE(d.has_value());
	EXPECT_EQ(d.value(), "hello");

	Result<std::string> e(std::string("y"));
	e = std::move(d);
	EXPECT_TRUE(e.has_value());
	EXPECT_EQ(e.value(), "hello");
}

TEST(ResultTest, MoveOnlyValue)
{
	Result<std::unique_ptr<int>> r(std::make_unique<int>(7));
	ASSERT_TRUE(r.has_value());
	EXPECT_EQ(*r.value(), 7);
}

TEST(ResultTest, VoidOkAndError)
{
	Result<void> ok;
	EXPECT_TRUE(ok.has_value());

	Result<void> err = std::unexpected(Status::InvalidArgument("bad"));
	EXPECT_FALSE(err.has_value());
	EXPECT_TRUE(err.error().IsInvalidArgument());
}

int main(int argc, char** argv)
{
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
