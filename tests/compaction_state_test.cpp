#include "db_impl.h"
#include "comparator.h"
#include "env.h"
#include "filename.h"

#include <filesystem>
#include <functional>
#include <gtest/gtest.h>
#include <string>
#include <thread>

using namespace prism;

namespace
{
	bool WaitUntil(const std::function<bool()>& condition, std::chrono::milliseconds timeout)
	{
		const auto deadline = std::chrono::steady_clock::now() + timeout;
		while (std::chrono::steady_clock::now() < deadline)
		{
			if (condition())
			{
				return true;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
		return condition();
	}
}

class CompactionStateTest: public ::testing::Test
{
protected:
	std::string dbname_;
	Options options_;

	void SetUp() override
	{
		options_.create_if_missing = true;
		options_.write_buffer_size = 4096;
		dbname_ = std::filesystem::temp_directory_path() / "compaction_state_test";
		std::filesystem::remove_all(dbname_);
	}

	void TearDown() override
	{
		std::filesystem::remove_all(dbname_);
	}
};

TEST_F(CompactionStateTest, QuiescentOnOpen)
{
	auto open = DBImpl::OpenInternal(options_, dbname_);
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());

	auto state = db->GetCompactionState();
	EXPECT_FALSE(state.compaction_in_flight);
	EXPECT_FALSE(state.flush_in_flight);
	EXPECT_FALSE(state.write_stalled);
	EXPECT_EQ(state.compaction_start_count, 0u);
	EXPECT_EQ(state.compaction_finish_count, 0u);
}

TEST_F(CompactionStateTest, FlushInFlightAfterFill)
{
	auto open = DBImpl::OpenInternal(options_, dbname_);
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());

	for (int i = 0; i < 4096; ++i)
	{
		ASSERT_TRUE(db->Put("k" + std::to_string(i), std::string(256, 'x')).ok());
		if (db->TEST_HasImmutableMemTable())
		{
			break;
		}
	}

	bool saw_flush = WaitUntil(
	    [&db] {
		    return db->GetCompactionState().flush_in_flight;
	    },
	    std::chrono::milliseconds(500));

	if (!saw_flush)
	{
		auto final_state = db->GetCompactionState();
		EXPECT_GE(final_state.compaction_start_count, 1u)
		    << "expected flush to either be in-flight or have completed (start_count advanced)";
	}
}

TEST_F(CompactionStateTest, CompactionQuiescesAfterActivity)
{
	auto open = DBImpl::OpenInternal(options_, dbname_);
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());

	for (int i = 0; i < 8; ++i)
	{
		for (int j = 0; j < 4096; ++j)
		{
			ASSERT_TRUE(db->Put("t" + std::to_string(i) + "_" + std::to_string(j), std::string(256, 'x')).ok());
			if (db->TEST_HasImmutableMemTable())
			{
				break;
			}
		}
		ASSERT_TRUE(WaitUntil(
		    [&db] {
			    return !db->TEST_HasImmutableMemTable();
		    },
		    std::chrono::seconds(5)));
	}

	ASSERT_TRUE(WaitUntil(
	    [&db] {
		    auto s = db->GetCompactionState();
		    return !s.compaction_in_flight && s.compaction_start_count > 0;
	    },
	    std::chrono::seconds(10)));

	auto final_state = db->GetCompactionState();
	EXPECT_FALSE(final_state.compaction_in_flight);
	EXPECT_GE(final_state.compaction_finish_count, 1u);
}
