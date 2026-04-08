#include "async_env.h"

#include <atomic>
#include <cstddef>
#include <filesystem>
#include <future>
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <vector>

#include "coro_task.h"
#include "env.h"
#include "slice.h"

using namespace prism;
using namespace prism::tests;

namespace
{

	class AsyncEnvTest: public ::testing::Test
	{
	protected:
		void SetUp() override
		{
			env_ = Env::Default();
			auto test_dir = env_->GetTestDirectory();
			ASSERT_TRUE(test_dir.has_value());
			dir_ = std::filesystem::path(test_dir.value()) / ("async_env_test-" + std::to_string(env_->NowMicros()));
			auto s = env_->CreateDir(dir_.string());
			ASSERT_TRUE(s.ok()) << s.ToString();
		}

		void TearDown() override
		{
			std::error_code ec;
			std::filesystem::remove_all(dir_, ec);
		}

		std::string TestFile(const std::string& name) const { return (dir_ / name).string(); }

		Env* env_ = nullptr;
		std::filesystem::path dir_;
	};

} // namespace

// Happy path: write a file via sync Env, read it back via AsyncEnv::ReadAtStringAsync.
TEST_F(AsyncEnvTest, ReadAtStringAsync)
{
	ThreadPoolScheduler scheduler(4);
	AsyncEnv async_env(scheduler, env_);

	const std::string path = TestFile("read_string.txt");

	// Write test data synchronously.
	{
		auto wf = env_->NewWritableFile(path);
		ASSERT_TRUE(wf.has_value()) << wf.error().ToString();
		auto s = wf.value()->Append(Slice("hello world"));
		ASSERT_TRUE(s.ok()) << s.ToString();
		s = wf.value()->Close();
		ASSERT_TRUE(s.ok()) << s.ToString();
	}

	auto task = [&]() -> Task<void> {
		auto file_res = co_await async_env.NewRandomAccessFileAsync(path);
		EXPECT_TRUE(file_res.has_value()) << file_res.error().ToString();
		if (!file_res.has_value())
			co_return;
		auto file = std::move(file_res.value());

		auto read_res = co_await file->ReadAtStringAsync(0, 11);
		EXPECT_TRUE(read_res.has_value()) << read_res.error().ToString();
		if (!read_res.has_value())
			co_return;
		EXPECT_EQ(read_res.value(), "hello world");
	}();
	task.SyncWait();
}

// Read via ReadAtAsync into a caller-owned std::vector<std::byte>.
TEST_F(AsyncEnvTest, ReadAtAsync)
{
	ThreadPoolScheduler scheduler(4);
	AsyncEnv async_env(scheduler, env_);

	const std::string path = TestFile("read_bytes.txt");
	const std::string kContent = "async_read";

	// Write test data synchronously.
	{
		auto wf = env_->NewWritableFile(path);
		ASSERT_TRUE(wf.has_value()) << wf.error().ToString();
		auto s = wf.value()->Append(Slice(kContent));
		ASSERT_TRUE(s.ok()) << s.ToString();
		s = wf.value()->Close();
		ASSERT_TRUE(s.ok()) << s.ToString();
	}

	// Use Task<string> to return the read result to T0 for verification,
	// avoiding EXPECT calls on the thread-pool thread (known ASan/coroutine FP).
	auto task = [p_env = &async_env, path, n = kContent.size()]() -> Task<std::string> {
		std::vector<std::byte> buf(n);
		auto file_res = co_await p_env->NewRandomAccessFileAsync(path);
		if (!file_res.has_value())
			co_return std::string{ };
		auto file = std::move(file_res.value());
		auto n_res = co_await file->ReadAtAsync(0, std::span<std::byte>(buf));
		if (!n_res.has_value() || n_res.value() == 0)
			co_return std::string{ };
		co_return std::string(reinterpret_cast<const char*>(buf.data()), n_res.value());
	}();
	std::string result = task.SyncWait();
	EXPECT_EQ(result, kContent);
}

// Partial read: request more bytes than the file contains.
// Partial read: request a range that matches actual file size.
TEST_F(AsyncEnvTest, ReadAtStringAsyncExact)
{
	ThreadPoolScheduler scheduler(4);
	AsyncEnv async_env(scheduler, env_);

	const std::string path = TestFile("read_exact.txt");

	{
		auto wf = env_->NewWritableFile(path);
		ASSERT_TRUE(wf.has_value()) << wf.error().ToString();
		auto s = wf.value()->Append(Slice("hi"));
		ASSERT_TRUE(s.ok()) << s.ToString();
		s = wf.value()->Close();
		ASSERT_TRUE(s.ok()) << s.ToString();
	}

	// Use Task<string> to return results for verification on T0.
	auto task = [p_env = &async_env, path]() -> Task<std::string> {
		auto file_res = co_await p_env->NewRandomAccessFileAsync(path);
		if (!file_res.has_value())
			co_return std::string{ };
		auto file = std::move(file_res.value());
		auto read_res = co_await file->ReadAtStringAsync(0, 2);
		if (!read_res.has_value())
			co_return std::string{ };
		co_return read_res.value();
	}();
	std::string result = task.SyncWait();
	EXPECT_EQ(result, "hi");
}

// Non-existent file should return an error, not crash.
TEST_F(AsyncEnvTest, NewRandomAccessFileAsyncFailure)
{
	ThreadPoolScheduler scheduler(4);
	AsyncEnv async_env(scheduler, env_);

	auto task = [&]() -> Task<void> {
		auto file_res = co_await async_env.NewRandomAccessFileAsync(TestFile("no_such_file.txt"));
		EXPECT_FALSE(file_res.has_value()) << "Expected failure for missing file";
	}();
	task.SyncWait();
}

// AppendAfterClose: append after CloseAsync returns IOError.
TEST_F(AsyncEnvTest, AppendAfterClose)
{
	ThreadPoolScheduler scheduler(4);
	AsyncEnv async_env(scheduler, env_);
	const std::string path = TestFile("append_after_close.txt");

	auto task = [&]() -> Task<void> {
		auto wf_res = co_await async_env.NewWritableFileAsync(path);
		EXPECT_TRUE(wf_res.has_value()) << wf_res.error().ToString();
		if (!wf_res.has_value())
			co_return;
		auto wf = std::move(wf_res.value());

		auto s = co_await wf->AppendAsync("hello");
		EXPECT_TRUE(s.ok()) << s.ToString();

		s = co_await wf->CloseAsync();
		EXPECT_TRUE(s.ok()) << s.ToString();

		// After close, further appends must fail.
		s = co_await wf->AppendAsync("world");
		EXPECT_FALSE(s.ok()) << "Expected IOError after close";
		EXPECT_TRUE(s.IsIOError()) << s.ToString();
	}();
	task.SyncWait();
}

// ConcurrentAppendFIFO: concurrent AppendAsync submissions preserve submission order.
// Uses a sequencing counter to ensure deterministic ordering without sleep_for.
TEST_F(AsyncEnvTest, ConcurrentAppendFIFO)
{
	ThreadPoolScheduler scheduler(8);
	const std::string path = TestFile("fifo_order.txt");
	constexpr int kCount = 16;

	// Create the writable file and wrap it.
	auto wf_res_raw = env_->NewWritableFile(path);
	ASSERT_TRUE(wf_res_raw.has_value()) << wf_res_raw.error().ToString();
	auto async_wf = std::make_shared<AsyncWritableFile>(scheduler, std::move(wf_res_raw.value()));

	// Ordered handoff synchronization: main thread serializes AppendAsync submissions.
	// Each worker thread has a 'go' flag and a 'ready' flag.
	// Main: release thread i -> wait for thread i to signal 'ready' -> release thread i+1.
	// This ensures AppendAsync tickets are obtained in strict 0..N-1 order.
	std::vector<std::atomic<bool>> go(kCount); // Signal to proceed
	std::vector<std::atomic<bool>> ready(kCount); // Ack that AppendAsync was called
	for (int i = 0; i < kCount; ++i)
	{
		go[i].store(false, std::memory_order_release);
		ready[i].store(false, std::memory_order_release);
	}

	// Launch all worker threads.
	std::vector<std::future<Status>> futures;
	futures.reserve(kCount);
	for (int i = 0; i < kCount; ++i)
	{
		int slot = i;
		futures.push_back(std::async(std::launch::async, [&async_wf, slot, &go, &ready]() -> Status {
			// Wait until main thread signals us to go.
			while (!go[slot].load(std::memory_order_acquire))
			{
				std::this_thread::yield();
			}

			// Call AppendAsync to obtain a ticket (deterministic order guaranteed by main thread).
			auto task = [](std::shared_ptr<AsyncWritableFile> wf, int s) -> Task<Status> {
				co_return co_await wf->AppendAsync(std::to_string(s) + "\n");
			}(async_wf, slot);

			// Signal main thread that we've entered AppendAsync (ticket obtained).
			ready[slot].store(true, std::memory_order_release);

			return task.SyncWait();
		}));
	}

	// Main thread enforces ordered submission: release thread i, wait for i to be ready, then release i+1.
	for (int i = 0; i < kCount; ++i)
	{
		// Release thread i.
		go[i].store(true, std::memory_order_release);

		// Spin-wait until thread i signals it has entered AppendAsync (obtained its ticket).
		while (!ready[i].load(std::memory_order_acquire))
		{
			std::this_thread::yield();
		}
	}

	// Collect results.
	for (int i = 0; i < kCount; ++i)
	{
		auto s = futures[i].get();
		EXPECT_TRUE(s.ok()) << "Append " << i << " failed: " << s.ToString();
	}

	// Close file.
	auto close_task = [](std::shared_ptr<AsyncWritableFile> wf) -> Task<Status> { co_return co_await wf->CloseAsync(); }(async_wf);
	auto cs = close_task.SyncWait();
	EXPECT_TRUE(cs.ok()) << cs.ToString();

	// Read back and verify order.
	auto rf = env_->NewRandomAccessFile(path);
	ASSERT_TRUE(rf.has_value()) << rf.error().ToString();
	auto file_size_res = env_->GetFileSize(path);
	ASSERT_TRUE(file_size_res.has_value());
	std::string content(file_size_res.value(), '\0');
	auto read_res = rf.value()->ReadAt(0, std::as_writable_bytes(std::span(content)));
	ASSERT_TRUE(read_res.has_value());

	std::string expected;
	for (int i = 0; i < kCount; ++i)
		expected += std::to_string(i) + "\n";
	EXPECT_EQ(content, expected);
}

// DestroyBeforeAwait: AsyncEnv wrapper is destroyed before the returned AsyncOp is awaited.
// The lambda inside the op captures scheduler_ and env_ by value (raw pointers from fields),
// so destroying AsyncEnv must NOT cause a use-after-free when the op later runs.
// Env::Default() and the scheduler (kept alive here) outlive the op.
TEST_F(AsyncEnvTest, DestroyBeforeAwait)
{
	ThreadPoolScheduler scheduler(4);
	const std::string path = TestFile("destroy_before_await.txt");

	// Write a file so the read can actually succeed.
	{
		auto wf = env_->NewWritableFile(path);
		ASSERT_TRUE(wf.has_value()) << wf.error().ToString();
		auto s = wf.value()->Append(Slice("data"));
		ASSERT_TRUE(s.ok()) << s.ToString();
		s = wf.value()->Close();
		ASSERT_TRUE(s.ok()) << s.ToString();
	}

	// Obtain the AsyncOp while async_env is alive, then destroy async_env.
	AsyncOp<Result<std::unique_ptr<AsyncRandomAccessFile>>> op = [&] {
		AsyncEnv async_env(scheduler, env_);
		return async_env.NewRandomAccessFileAsync(path);
	}(); // async_env destroyed here

	// Await the op — must not crash or UAF even though AsyncEnv wrapper is gone.
	auto task = [](AsyncOp<Result<std::unique_ptr<AsyncRandomAccessFile>>> o) -> Task<bool> {
		auto res = co_await std::move(o);
		co_return res.has_value();
	}(std::move(op));
	bool ok = task.SyncWait();
	EXPECT_TRUE(ok) << "DestroyBeforeAwait: op failed unexpectedly";
}

// CloseInterleaving: concurrent Close and Append submissions — the one that gets a ticket
// first wins; the other must return IOError (if close was first) or succeed (if append was first).
// Either way, no deadlock, no crash.
TEST_F(AsyncEnvTest, CloseInterleaving)
{
	ThreadPoolScheduler scheduler(8);
	const std::string path = TestFile("close_interleave.txt");

	auto wf_raw = env_->NewWritableFile(path);
	ASSERT_TRUE(wf_raw.has_value()) << wf_raw.error().ToString();
	auto async_wf = std::make_shared<AsyncWritableFile>(scheduler, std::move(wf_raw.value()));

	// Issue CloseAsync and AppendAsync concurrently from two threads.
	// Both get tickets under the mutex — whichever ran first determines the outcome.
	auto close_future = std::async(std::launch::async, [&async_wf]() -> Status {
		auto task = [](std::shared_ptr<AsyncWritableFile> wf) -> Task<Status> { co_return co_await wf->CloseAsync(); }(async_wf);
		return task.SyncWait();
	});

	auto append_future = std::async(std::launch::async, [&async_wf]() -> Status {
		auto task
		    = [](std::shared_ptr<AsyncWritableFile> wf) -> Task<Status> { co_return co_await wf->AppendAsync("interleaved"); }(async_wf);
		return task.SyncWait();
	});

	Status close_status = close_future.get();
	Status append_status = append_future.get();

	// Close must always succeed (file was open).
	EXPECT_TRUE(close_status.ok()) << "Close failed: " << close_status.ToString();

	// Append either succeeded (ticket before close) or returned IOError (ticket after close).
	bool ok_or_error = append_status.ok() || append_status.IsIOError();
	EXPECT_TRUE(ok_or_error) << "Unexpected append status: " << append_status.ToString();
}

TEST_F(AsyncEnvTest, QueuedSecondCloseReturnsIoError)
{
	ThreadPoolScheduler scheduler(4);
	const std::string path = TestFile("queued_second_close.txt");

	auto wf_raw = env_->NewWritableFile(path);
	ASSERT_TRUE(wf_raw.has_value()) << wf_raw.error().ToString();
	auto async_wf = std::make_shared<AsyncWritableFile>(scheduler, std::move(wf_raw.value()));

	auto first_close = async_wf->CloseAsync();
	auto second_close = async_wf->CloseAsync();

	auto first_task = [](AsyncOp<Status> op) -> Task<Status> { co_return co_await std::move(op); }(std::move(first_close));
	auto second_task = [](AsyncOp<Status> op) -> Task<Status> { co_return co_await std::move(op); }(std::move(second_close));

	Status first = first_task.SyncWait();
	Status second = second_task.SyncWait();

	EXPECT_TRUE(first.ok()) << first.ToString();
	EXPECT_TRUE(second.IsIOError()) << "Second queued CloseAsync should fail deterministically: " << second.ToString();
}
