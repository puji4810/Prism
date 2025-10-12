#ifndef PRISM_UTIL_RANDOM_H
#define PRISM_UTIL_RANDOM_H

#include <cstdint>

namespace prism
{
	class Random
	{
	private:
		uint32_t seed_;

	public:
		explicit Random(uint32_t seed): seed_(seed & 0x7fffffffu) {
			if (seed_ == 0 || seed_ == 2147483647L) {
				seed_ = 1;
			}
		}
		
		uint32_t Next() {
			static const uint32_t M = 2147483647L; // 2^31-1
			static const uint64_t A = 16807; // bits 14, 8, 7, 5, 2, 1, 0
            // Make sure that every number in [1,M-1] is equally likely
			uint64_t product = seed_ * A;

			// https://stackoverflow.com/questions/50107788/random-number-generator-in-c-for-skiplist
			// https://en.wikipedia.org/wiki/Lehmer_random_number_generator
			// We know that the result of (p >> 31 + p & M) is equivalent to p % M
			// using the fact that ((x << 31) % M) == x
			// For A uint64_t P, it can be represented as
			// P = q * 2^31 + r
			// where q is the quotient and r is the remainder
			// P mod M = (q * 2^31 + r) mod M
			// as q * 2^31 mod M = q, we have
			// P mod M = (q + r) mod M
			seed_ = static_cast<uint32_t>((product >> 31) + (product & M));

			// The first reduction may overflow by 1 bit, so we may need to
			// repeat. mod == M is not possible; using > allows the faster
			// sign-bit-based test.
			if (seed_ > M) {
				seed_ -= M;
			}
			return seed_;
		}

		// Returns a uniformly distributed value in the range [0..n-1]
		// REQUIRES: n > 0
		uint32_t Uniform(int n) { return Next() % n; }

		// Randomly returns true ~"1/n" of the time, and false otherwise.
		// REQUIRES: n > 0
		bool OneIn(int n) { return (Next() % n) == 0; }
		
		// Skewed: pick "base" uniformly from range [0,max_log] and then
		// return "base" random bits.  The effect is to pick a number in the
		// range [0,2^max_log-1] with exponential bias towards smaller numbers.
		uint32_t Skewed(int max_log) { return Uniform(1 << Uniform(max_log + 1)); }
	};
}

#endif