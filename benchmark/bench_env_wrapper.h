#ifndef PRISM_BENCH_BENCH_ENV_WRAPPER_H_
#define PRISM_BENCH_BENCH_ENV_WRAPPER_H_

#include "env.h"

#include <atomic>

namespace prism::bench
{

	// Benchmark-only EnvWrapper that counts sleep calls.
	// This is NOT a production class - it lives only in benchmark/ for observing
	// background scheduling behavior in compaction scenarios.
	class BenchEnvWrapper final: public EnvWrapper
	{
	public:
		explicit BenchEnvWrapper(Env* target)
		    : EnvWrapper(target)
		{
		}

		int SleepCalls() const { return sleep_calls_.load(std::memory_order_acquire); }
		void Reset()
		{
			sleep_calls_.store(0, std::memory_order_release);
		}

		void SleepForMicroseconds(int micros) override
		{
			sleep_calls_.fetch_add(1, std::memory_order_release);
			target()->SleepForMicroseconds(micros);
		}

	private:
		std::atomic<int> sleep_calls_{ 0 };
	};

} // namespace prism::bench

#endif // PRISM_BENCH_BENCH_ENV_WRAPPER_H_