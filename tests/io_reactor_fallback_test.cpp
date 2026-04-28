#include "../src/io_reactor.h"
#include "../src/runtime_executor.h"
#include "../src/runtime_metrics.h"

#include "async_env.h"
#include "coro_task.h"
#include "gtest/gtest.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <semaphore>
#include <string>
#include <string_view>
#include <utility>

#include <fcntl.h>
#include <unistd.h>

using namespace prism;
using namespace prism::tests;
using namespace std::chrono_literals;

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

	private:
		std::filesystem::path path_;
	};

	struct ScopedFd
	{
		explicit ScopedFd(int fd)
		    : fd_(fd)
		{
		}

		~ScopedFd()
		{
			if (fd_ >= 0)
			{
				::close(fd_);
			}
		}

		ScopedFd(const ScopedFd&) = delete;
		ScopedFd& operator=(const ScopedFd&) = delete;

		int get() const noexcept { return fd_; }

	private:
		int fd_;
	};

	class InvalidReactor final: public IIoReactor
	{
	public:
		bool IsValid() const noexcept override { return false; }
		bool SubmitRead(int, void*, unsigned, off_t, uint64_t) override { return false; }
		int WaitCompletion(uint64_t*, int*) override { return 0; }
	};

	class UnsupportedReadReactor final: public IIoReactor
	{
	public:
		bool IsValid() const noexcept override { return true; }

		bool SubmitRead(int, void*, unsigned, off_t, uint64_t) override
		{
			submit_count_.fetch_add(1, std::memory_order_relaxed);
			return false;
		}

		int WaitCompletion(uint64_t*, int*) override { return 0; }

		int submit_count() const noexcept { return submit_count_.load(std::memory_order_relaxed); }

	private:
		std::atomic<int> submit_count_{ 0 };
	};

	class MemoryRandomAccessFile final: public RandomAccessFile
	{
	public:
		explicit MemoryRandomAccessFile(std::string contents)
		    : contents_(std::move(contents))
		{
		}

		Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override
		{
			if (offset >= contents_.size())
			{
				*result = Slice();
				return Status::OK();
			}

			const std::size_t read_size = std::min<std::size_t>(n, contents_.size() - static_cast<std::size_t>(offset));
			std::memcpy(scratch, contents_.data() + offset, read_size);
			*result = Slice(scratch, read_size);
			return Status::OK();
		}

	private:
		std::string contents_;
	};

	std::filesystem::path UniqueTestDir()
	{
		const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
		return std::filesystem::temp_directory_path() / ("io_reactor_fallback_test-" + std::to_string(stamp));
	}

	void WriteAll(int fd, std::string_view contents)
	{
		std::size_t written = 0;
		while (written < contents.size())
		{
			const ssize_t result = ::write(fd, contents.data() + written, contents.size() - written);
			ASSERT_GT(result, 0) << std::strerror(errno);
			written += static_cast<std::size_t>(result);
		}
	}

	class IoReactorFallbackTest: public ::testing::Test
	{
	protected:
		void SetUp() override { RuntimeMetrics::Instance().Reset(); }
	};
}

TEST_F(IoReactorFallbackTest, ProbeReturnsUnavailableWhenIoUringNotSupported)
{
	BlockingExecutor executor;
	IoReadDispatcher dispatcher(executor, IoCapability::kUnavailable, nullptr);

	const std::string contents = "fallback-from-probe";
	const auto test_dir = UniqueTestDir();
	const ScopedRemoveAll cleanup(test_dir);
	ASSERT_FALSE(test_dir.empty());
	ASSERT_TRUE(std::filesystem::create_directories(test_dir) || std::filesystem::exists(test_dir));
	const auto file_path = test_dir / "probe.txt";
	{
		ScopedFd write_fd(::open(file_path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644));
		ASSERT_GE(write_fd.get(), 0) << std::strerror(errno);
		WriteAll(write_fd.get(), contents);
	}

	ScopedFd read_fd(::open(file_path.c_str(), O_RDONLY));
	ASSERT_GE(read_fd.get(), 0) << std::strerror(errno);

	std::array<char, 64> buffer{ };
	std::binary_semaphore done(0);
	std::atomic<uint64_t> user_data{ 0 };
	std::atomic<int> result{ -1 };

	dispatcher.SubmitRead(read_fd.get(),
	    buffer.data(),
	    contents.size(),
	    0,
	    7,
	    [&](uint64_t completed_user_data, int completed_result) {
			user_data.store(completed_user_data, std::memory_order_release);
			result.store(completed_result, std::memory_order_release);
			done.release();
	    });

	ASSERT_TRUE(done.try_acquire_for(5s));
	EXPECT_EQ(user_data.load(std::memory_order_acquire), 7u);
	EXPECT_EQ(result.load(std::memory_order_acquire), static_cast<int>(contents.size()));
	EXPECT_EQ(std::string_view(buffer.data(), contents.size()), contents);
	EXPECT_GE(RuntimeMetrics::Instance().fallback_to_blocking_count.load(std::memory_order_relaxed), 1u);
}

TEST_F(IoReactorFallbackTest, InitFailureRecordsFallbackCounter)
{
	BlockingExecutor executor;
	IoReadDispatcher dispatcher(executor, IoCapability::kSupported, std::make_unique<InvalidReactor>());
	EXPECT_FALSE(dispatcher.HasReactor());
	EXPECT_GE(RuntimeMetrics::Instance().fallback_to_blocking_count.load(std::memory_order_relaxed), 1u);
}

TEST_F(IoReactorFallbackTest, FallbackToBlockingExecutorForReads)
{
	BlockingExecutor executor;
	auto reactor = std::make_unique<UnsupportedReadReactor>();
	auto* reactor_ptr = reactor.get();
	IoReadDispatcher dispatcher(executor, IoCapability::kSupported, std::move(reactor));

	const std::string contents = "fallback-from-unsupported-op";
	const auto test_dir = UniqueTestDir();
	const ScopedRemoveAll cleanup(test_dir);
	ASSERT_FALSE(test_dir.empty());
	ASSERT_TRUE(std::filesystem::create_directories(test_dir) || std::filesystem::exists(test_dir));
	const auto file_path = test_dir / "unsupported.txt";
	{
		ScopedFd write_fd(::open(file_path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644));
		ASSERT_GE(write_fd.get(), 0) << std::strerror(errno);
		WriteAll(write_fd.get(), contents);
	}

	ScopedFd read_fd(::open(file_path.c_str(), O_RDONLY));
	ASSERT_GE(read_fd.get(), 0) << std::strerror(errno);

	std::array<char, 64> buffer{ };
	std::binary_semaphore done(0);
	std::atomic<uint64_t> user_data{ 0 };
	std::atomic<int> result{ -1 };

	dispatcher.SubmitRead(read_fd.get(),
	    buffer.data(),
	    contents.size(),
	    0,
	    13,
	    [&](uint64_t completed_user_data, int completed_result) {
			user_data.store(completed_user_data, std::memory_order_release);
			result.store(completed_result, std::memory_order_release);
			done.release();
	    });

	ASSERT_TRUE(done.try_acquire_for(5s));
	EXPECT_EQ(reactor_ptr->submit_count(), 1);
	EXPECT_EQ(user_data.load(std::memory_order_acquire), 13u);
	EXPECT_EQ(result.load(std::memory_order_acquire), static_cast<int>(contents.size()));
	EXPECT_EQ(std::string_view(buffer.data(), contents.size()), contents);
	EXPECT_GE(RuntimeMetrics::Instance().fallback_to_blocking_count.load(std::memory_order_relaxed), 1u);
}

TEST_F(IoReactorFallbackTest, AsyncRandomAccessFileFallsBackWhenReactorUnavailable)
{
	ThreadPoolScheduler scheduler(4);
	auto runtime = AcquireRuntimeBundle(scheduler);
	runtime->io_reactor = nullptr;

	const std::string contents = "fallback-async-random-read";
	AsyncRandomAccessFile async_file(scheduler, std::make_shared<MemoryRandomAccessFile>(contents));

	auto task = [&]() -> Task<std::string> {
		auto read_result = co_await async_file.ReadAtStringAsync(0, contents.size());
		if (!read_result.has_value())
		{
			co_return std::string{ };
		}
		co_return read_result.value();
	}();

	EXPECT_EQ(task.SyncWait(), contents);
	EXPECT_GE(RuntimeMetrics::Instance().fallback_to_blocking_count.load(std::memory_order_relaxed), 1u);
}

TEST_F(IoReactorFallbackTest, AsyncEnvFactoryAndMetadataOpsRouteThroughUnifiedBackendSelect)
{
	ThreadPoolScheduler scheduler(4);
	auto runtime = AcquireRuntimeBundle(scheduler);
	runtime->io_reactor = nullptr;

	Env* env = Env::Default();
	ASSERT_NE(env, nullptr);
	const auto test_dir = UniqueTestDir();
	const ScopedRemoveAll cleanup(test_dir);
	ASSERT_TRUE(std::filesystem::create_directories(test_dir) || std::filesystem::exists(test_dir));

	AsyncEnv async_env(scheduler, env);
	const std::string subdir = (test_dir / "nested").string();
	const std::string file_path = (test_dir / "backend_select.txt").string();
	const std::string payload = "factory-metadata-routing";

	auto task = [](AsyncEnv* p_async_env, std::string subdir, std::string file_path, std::string payload) -> Task<void> {
		auto s = co_await p_async_env->CreateDirAsync(subdir);
		EXPECT_TRUE(s.ok()) << s.ToString();

		auto writable = co_await p_async_env->NewWritableFileAsync(file_path);
		EXPECT_TRUE(writable.has_value()) << writable.error().ToString();
		if (!writable.has_value())
		{
			co_return;
		}
		auto wf = std::move(writable.value());

		s = co_await wf.AppendAsync(payload);
		EXPECT_TRUE(s.ok()) << s.ToString();
		s = co_await wf.CloseAsync();
		EXPECT_TRUE(s.ok()) << s.ToString();

		auto fallback_after_write_factory = RuntimeMetrics::Instance().fallback_to_blocking_count.load(std::memory_order_relaxed);
		EXPECT_EQ(fallback_after_write_factory, 0u);

		auto size_result = co_await p_async_env->GetFileSizeAsync(file_path);
		EXPECT_TRUE(size_result.has_value()) << size_result.error().ToString();
		if (!size_result.has_value())
		{
			co_return;
		}
		EXPECT_EQ(size_result.value(), payload.size());

		auto appendable = co_await p_async_env->NewAppendableFileAsync(file_path);
		EXPECT_TRUE(appendable.has_value()) << appendable.error().ToString();
		if (!appendable.has_value())
		{
			co_return;
		}
		auto append_file = std::move(appendable.value());

		s = co_await append_file.CloseAsync();
		EXPECT_TRUE(s.ok()) << s.ToString();

		auto random_access = co_await p_async_env->NewRandomAccessFileAsync(file_path);
		EXPECT_TRUE(random_access.has_value()) << random_access.error().ToString();
		if (!random_access.has_value())
		{
			co_return;
		}
		EXPECT_GE(RuntimeMetrics::Instance().fallback_to_blocking_count.load(std::memory_order_relaxed), 1u);

		s = co_await p_async_env->RemoveFileAsync(file_path);
		EXPECT_TRUE(s.ok()) << s.ToString();
	}(&async_env, subdir, file_path, payload);

	task.SyncWait();
}
