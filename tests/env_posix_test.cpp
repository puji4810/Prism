#include "gtest/gtest.h"

#include "env.h"
#include "slice.h"
#include "status.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <chrono>
#include <algorithm>
#include <mutex>
#include <string>
#include <vector>

using namespace prism;

namespace
{
	struct ScopedRemoveAll
	{
		explicit ScopedRemoveAll(std::filesystem::path path)
		    : path_(std::move(path))
		{
		}

		~ScopedRemoveAll()
		{
			std::error_code ec;
			std::filesystem::remove_all(path_, ec);
		}

		ScopedRemoveAll(const ScopedRemoveAll&) = delete;
		ScopedRemoveAll& operator=(const ScopedRemoveAll&) = delete;

	private:
		std::filesystem::path path_;
	};

	static std::filesystem::path UniqueTestDir(Env* env)
	{
		std::string base_dir;
		EXPECT_TRUE(env->GetTestDirectory(&base_dir).ok());
		return std::filesystem::path(base_dir) / ("env_posix_test-" + std::to_string(env->NowMicros()));
	}

	struct ScheduleState
	{
		std::atomic<int> done{ 0 };
		std::mutex mu;
		std::condition_variable cv;
	};

	static void ScheduleCallback(void* arg)
	{
		auto* state = reinterpret_cast<ScheduleState*>(arg);
		const int now_done = state->done.fetch_add(1, std::memory_order_relaxed) + 1;
		if (now_done >= 1)
		{
			std::lock_guard<std::mutex> guard(state->mu);
			state->cv.notify_all();
		}
	}
}

TEST(PosixEnvTest, DefaultIsSingleton)
{
	Env* a = Env::Default();
	Env* b = Env::Default();
	EXPECT_EQ(a, b);
}

TEST(PosixEnvTest, FileAndDirectoryOps)
{
	Env* env = Env::Default();
	const auto test_dir = UniqueTestDir(env);
	const ScopedRemoveAll cleanup(test_dir);

	ASSERT_TRUE(env->CreateDir(test_dir.string()).ok());
	EXPECT_TRUE(env->FileExists(test_dir.string()));

	const auto file_path = (test_dir / "a.txt").string();

	auto result = env->NewWritableFile(file_path);
	ASSERT_TRUE(result.has_value());
	auto wf = std::move(result.value());
	ASSERT_NE(wf, nullptr);
	ASSERT_TRUE(wf->Append(Slice("hello")).ok());
	ASSERT_TRUE(wf->Close().ok());

	EXPECT_TRUE(env->FileExists(file_path));

	uint64_t file_size = 0;
	auto file_size_result = env->GetFileSize(file_path);
	ASSERT_TRUE(file_size_result.has_value());
	file_size = file_size_result.value();
	EXPECT_EQ(file_size, 5u);

	SequentialFile* sf = nullptr;
	auto r = env->NewSequentialFile(file_path);
	ASSERT_TRUE(r);
	sf = std::move(r)->release();
	ASSERT_NE(sf, nullptr);
	char scratch[32];
	Slice got;
	ASSERT_TRUE(sf->Read(sizeof(scratch), &got, scratch).ok());
	EXPECT_EQ(got.ToString(), "hello");
	delete sf;

	WritableFile* af = nullptr;
	auto appendable_result = env->NewAppendableFile(file_path);
	ASSERT_TRUE(appendable_result.has_value());
	af = std::move(appendable_result.value()).release();
	ASSERT_NE(af, nullptr);
	ASSERT_TRUE(af->Append(Slice(" world")).ok());
	ASSERT_TRUE(af->Close().ok());
	delete af;

	std::string contents;
	ASSERT_TRUE(ReadFileToString(env, file_path, &contents).ok());
	EXPECT_EQ(contents, "hello world");

	std::vector<std::string> children;
	auto children_result = env->GetChildren(test_dir.string());
	ASSERT_TRUE(children_result.has_value());
	children = std::move(children_result.value());
	EXPECT_NE(std::find(children.begin(), children.end(), "a.txt"), children.end());

	const auto renamed = (test_dir / "b.txt").string();
	ASSERT_TRUE(env->RenameFile(file_path, renamed).ok());
	EXPECT_FALSE(env->FileExists(file_path));
	EXPECT_TRUE(env->FileExists(renamed));

	ASSERT_TRUE(env->RemoveFile(renamed).ok());
	EXPECT_FALSE(env->FileExists(renamed));

	ASSERT_TRUE(env->RemoveDir(test_dir.string()).ok());
}

TEST(PosixEnvTest, FileLock)
{
	Env* env = Env::Default();
	const auto test_dir = UniqueTestDir(env);
	const ScopedRemoveAll cleanup(test_dir);

	ASSERT_TRUE(env->CreateDir(test_dir.string()).ok());

	const auto lock_path = (test_dir / "LOCK").string();
	FileLock* lock1 = nullptr;
	ASSERT_TRUE(env->LockFile(lock_path, &lock1).ok());
	ASSERT_NE(lock1, nullptr);

	FileLock* lock2 = nullptr;
	Status s = env->LockFile(lock_path, &lock2);
	EXPECT_FALSE(s.ok());
	EXPECT_EQ(lock2, nullptr);

	ASSERT_TRUE(env->UnlockFile(lock1).ok());

	FileLock* lock3 = nullptr;
	ASSERT_TRUE(env->LockFile(lock_path, &lock3).ok());
	ASSERT_NE(lock3, nullptr);
	ASSERT_TRUE(env->UnlockFile(lock3).ok());
}

TEST(PosixEnvTest, LoggerWrites)
{
	Env* env = Env::Default();
	const auto test_dir = UniqueTestDir(env);
	const ScopedRemoveAll cleanup(test_dir);

	ASSERT_TRUE(env->CreateDir(test_dir.string()).ok());

	const auto log_path = (test_dir / "LOG").string();
	Logger* logger = nullptr;
	ASSERT_TRUE(env->NewLogger(log_path, &logger).ok());
	ASSERT_NE(logger, nullptr);
	Log(logger, "hello %d", 123);
	delete logger;

	std::string contents;
	ASSERT_TRUE(ReadFileToString(env, log_path, &contents).ok());
	EXPECT_NE(contents.find("hello 123"), std::string::npos);
}

TEST(PosixEnvTest, ScheduleRuns)
{
	Env* env = Env::Default();
	ScheduleState state;

	env->Schedule(&ScheduleCallback, &state);
	env->Schedule(&ScheduleCallback, &state);
	env->Schedule(&ScheduleCallback, &state);

	std::unique_lock<std::mutex> lock(state.mu);
	const bool finished = state.cv.wait_for(lock, std::chrono::seconds(2), [&]() { return state.done.load() == 3; });
	EXPECT_TRUE(finished);
}

TEST(PosixEnvTest, NowMicrosAndSleep)
{
	Env* env = Env::Default();
	const uint64_t before = env->NowMicros();
	env->SleepForMicroseconds(2000);
	const uint64_t after = env->NowMicros();
	EXPECT_GT(after, before);
}

int main(int argc, char** argv)
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
