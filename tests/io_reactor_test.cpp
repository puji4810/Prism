#include "../src/io_reactor.h"
#include "../src/runtime_executor.h"

#include "async_env.h"
#include "coro_task.h"
#include "env.h"

#include "gtest/gtest.h"

#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <string_view>
#include <utility>

#include <fcntl.h>
#include <unistd.h>

using namespace prism;
using namespace prism::tests;

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

	std::filesystem::path UniqueTestDir()
	{
		const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
		return std::filesystem::temp_directory_path() / ("io_reactor_test-" + std::to_string(stamp));
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
}

TEST(IoReactorTest, IoReactorInitSucceedsOnSupportedKernel)
{
	if (IoReactor::Probe() == IoCapability::kUnavailable)
	{
		GTEST_SKIP() << "io_uring unavailable on this runtime";
	}

	IoReactor reactor;
	EXPECT_TRUE(reactor.IsValid());
	EXPECT_FALSE(reactor.IsFallback());
}

TEST(IoReactorTest, IoReactorBasicReadCompletes)
{
	if (IoReactor::Probe() == IoCapability::kUnavailable)
	{
		GTEST_SKIP() << "io_uring unavailable on this runtime";
	}

	IoReactor reactor;
	ASSERT_TRUE(reactor.IsValid());

	const auto test_dir = UniqueTestDir();
	const ScopedRemoveAll cleanup(test_dir);
	ASSERT_FALSE(test_dir.empty());
	ASSERT_TRUE(std::filesystem::create_directories(test_dir) || std::filesystem::exists(test_dir));

	const auto file_path = test_dir / "reactor.txt";
	const std::string contents = "reactor-read-path";
	{
		ScopedFd write_fd(::open(file_path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644));
		ASSERT_GE(write_fd.get(), 0) << std::strerror(errno);
		WriteAll(write_fd.get(), contents);
	}

	ScopedFd read_fd(::open(file_path.c_str(), O_RDONLY));
	ASSERT_GE(read_fd.get(), 0) << std::strerror(errno);

	std::array<char, 64> buffer{ };
	constexpr uint64_t kUserData = 99;
	if (!reactor.SubmitRead(read_fd.get(), buffer.data(), contents.size(), 0, kUserData))
	{
		GTEST_SKIP() << "io_uring read opcode unavailable on this kernel";
	}

	uint64_t user_data = 0;
	int result = 0;
	ASSERT_EQ(reactor.WaitCompletion(&user_data, &result), 1);
	EXPECT_EQ(user_data, kUserData);
	ASSERT_GE(result, 0);
	EXPECT_EQ(result, static_cast<int>(contents.size()));
	EXPECT_EQ(std::string_view(buffer.data(), contents.size()), contents);
}

TEST(IoReactorTest, IoReactorMoveSemantics)
{
	if (IoReactor::Probe() == IoCapability::kUnavailable)
	{
		GTEST_SKIP() << "io_uring unavailable on this runtime";
	}

	IoReactor reactor;
	ASSERT_TRUE(reactor.IsValid());

	IoReactor moved(std::move(reactor));
	EXPECT_FALSE(reactor.IsValid());
	EXPECT_TRUE(moved.IsValid());
	EXPECT_FALSE(moved.IsFallback());
}

TEST(IoReactorTest, AsyncRandomAccessFileUsesReactorBackend)
{
	if (IoReactor::Probe() == IoCapability::kUnavailable)
	{
		GTEST_SKIP() << "io_uring unavailable on this runtime";
	}

	ThreadPoolScheduler scheduler(4);
	auto runtime = AcquireRuntimeBundle(scheduler);
	IoReactor reactor;
	ASSERT_TRUE(reactor.IsValid());
	runtime->io_reactor = &reactor;

	Env* env = Env::Default();
	const auto test_dir = UniqueTestDir();
	const ScopedRemoveAll cleanup(test_dir);
	ASSERT_FALSE(test_dir.empty());
	ASSERT_TRUE(std::filesystem::create_directories(test_dir) || std::filesystem::exists(test_dir));

	const auto file_path = test_dir / "async_env_reactor.txt";
	const std::string contents = "async-random-read-reactor";
	{
		ScopedFd write_fd(::open(file_path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644));
		ASSERT_GE(write_fd.get(), 0) << std::strerror(errno);
		WriteAll(write_fd.get(), contents);
	}

	auto file_result = env->NewRandomAccessFile(file_path.string());
	ASSERT_TRUE(file_result.has_value()) << file_result.error().ToString();
	AsyncRandomAccessFile async_file(scheduler, std::shared_ptr<RandomAccessFile>(std::move(file_result.value())));

	auto task = [&]() -> Task<std::string> {
		auto read_result = co_await async_file.ReadAtStringAsync(0, contents.size());
		if (!read_result.has_value())
		{
			co_return std::string{ };
		}
		co_return read_result.value();
	}();

	EXPECT_EQ(task.SyncWait(), contents);
	runtime->io_reactor = nullptr;
}
