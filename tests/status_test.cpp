#include "gtest/gtest.h"
#include "status.h"

using namespace prism;

TEST(StatusTest, OK)
{
	Status s = Status::OK();
	EXPECT_TRUE(s.ok());
	EXPECT_FALSE(s.IsNotFound());
	EXPECT_FALSE(s.IsCorruption());
	EXPECT_FALSE(s.IsIOError());
	EXPECT_EQ("OK", s.ToString());
}

TEST(StatusTest, NotFound)
{
	Status s = Status::NotFound("custom message");
	EXPECT_FALSE(s.ok());
	EXPECT_TRUE(s.IsNotFound());
	EXPECT_FALSE(s.IsCorruption());
	EXPECT_FALSE(s.IsIOError());
	EXPECT_EQ("NotFound: custom message", s.ToString());
}

TEST(StatusTest, NotFoundWithMsg2)
{
	Status s = Status::NotFound("msg1", "msg2");
	EXPECT_FALSE(s.ok());
	EXPECT_TRUE(s.IsNotFound());
	EXPECT_EQ("NotFound: msg1: msg2", s.ToString());
}

TEST(StatusTest, Corruption)
{
	Status s = Status::Corruption("file corrupted");
	EXPECT_FALSE(s.ok());
	EXPECT_FALSE(s.IsNotFound());
	EXPECT_TRUE(s.IsCorruption());
	EXPECT_FALSE(s.IsIOError());
	EXPECT_EQ("Corruption: file corrupted", s.ToString());
}

TEST(StatusTest, IOError)
{
	Status s = Status::IOError("read failed", "file.txt");
	EXPECT_FALSE(s.ok());
	EXPECT_FALSE(s.IsNotFound());
	EXPECT_FALSE(s.IsCorruption());
	EXPECT_TRUE(s.IsIOError());
	EXPECT_EQ("IO error: read failed: file.txt", s.ToString());
}

TEST(StatusTest, InvalidArgument)
{
	Status s = Status::InvalidArgument("bad key");
	EXPECT_FALSE(s.ok());
	EXPECT_TRUE(s.IsInvalidArgument());
	EXPECT_EQ("Invalid argument: bad key", s.ToString());
}

TEST(StatusTest, NotSupported)
{
	Status s = Status::NotSupported("operation xyz");
	EXPECT_FALSE(s.ok());
	EXPECT_TRUE(s.IsNotSupportedError());
	EXPECT_EQ("Not implemented: operation xyz", s.ToString());
}

TEST(StatusTest, CopyConstructor)
{
	Status s1 = Status::NotFound("error message");
	Status s2(s1);
	EXPECT_TRUE(s1.IsNotFound());
	EXPECT_TRUE(s2.IsNotFound());
	EXPECT_EQ(s1.ToString(), s2.ToString());
}

TEST(StatusTest, CopyAssignment)
{
	Status s1 = Status::IOError("io error");
	Status s2 = Status::OK();
	s2 = s1;
	EXPECT_TRUE(s1.IsIOError());
	EXPECT_TRUE(s2.IsIOError());
	EXPECT_EQ(s1.ToString(), s2.ToString());
}

TEST(StatusTest, MoveConstructor)
{
	Status s1 = Status::Corruption("corrupted");
	Status s2(std::move(s1));
	EXPECT_TRUE(s2.IsCorruption());
	EXPECT_EQ("Corruption: corrupted", s2.ToString());
	// s1 should be in valid but unspecified state after move
}

TEST(StatusTest, MoveAssignment)
{
	Status s1 = Status::InvalidArgument("arg error");
	Status s2 = Status::OK();
	s2 = std::move(s1);
	EXPECT_TRUE(s2.IsInvalidArgument());
	EXPECT_EQ("Invalid argument: arg error", s2.ToString());
}

TEST(StatusTest, SelfAssignment)
{
	Status s = Status::NotFound("test");
	s = s; // Self-assignment should be safe
	EXPECT_TRUE(s.IsNotFound());
	EXPECT_EQ("NotFound: test", s.ToString());
}

int main(int argc, char** argv)
{
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
