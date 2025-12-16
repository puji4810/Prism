#include "gtest/gtest.h"

#include "result.h"

#include <memory>
#include <string>

using namespace prism;

namespace
{
	Result<int> OkInt() { return 123; }

	Result<int> ErrInt() { return Status::NotFound("missing"); }
}

TEST(ResultTest, Value)
{
	Result<int> r = OkInt();
	EXPECT_TRUE(r.ok());
	EXPECT_TRUE(r.status().ok());
	EXPECT_EQ(r.value(), 123);
}

TEST(ResultTest, Error)
{
	Result<int> r = ErrInt();
	EXPECT_FALSE(r.ok());
	EXPECT_TRUE(r.status().IsNotFound());
}

TEST(ResultTest, CopyAndMove)
{
	Result<std::string> a(std::string("hello"));
	Result<std::string> b(a);
	EXPECT_TRUE(b.ok());
	EXPECT_EQ(b.value(), "hello");

	Result<std::string> c(std::move(b));
	EXPECT_TRUE(c.ok());
	EXPECT_EQ(c.value(), "hello");

	Result<std::string> d(std::string("x"));
	d = a;
	EXPECT_TRUE(d.ok());
	EXPECT_EQ(d.value(), "hello");

	Result<std::string> e(std::string("y"));
	e = std::move(d);
	EXPECT_TRUE(e.ok());
	EXPECT_EQ(e.value(), "hello");
}

TEST(ResultTest, MoveOnlyValue)
{
	Result<std::unique_ptr<int>> r(std::make_unique<int>(7));
	ASSERT_TRUE(r.ok());
	EXPECT_EQ(*r.value(), 7);
}

TEST(ResultTest, VoidOkAndError)
{
	Result<void> ok;
	EXPECT_TRUE(ok.ok());

	Result<void> err = Status::InvalidArgument("bad");
	EXPECT_FALSE(err.ok());
	EXPECT_TRUE(err.status().IsInvalidArgument());
}

int main(int argc, char** argv)
{
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
