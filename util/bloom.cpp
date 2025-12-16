#include "filter_policy.h"
#include "hash.h"
#include <cstddef>
#include <cstdint>

namespace prism
{
	namespace
	{
		static uint32_t BloomHash(const Slice& key) { return Hash(key.data(), key.size(), 0xbc9f1d34); }
	}

	class BloomFilterPolicy: public FilterPolicy
	{
	public:
		explicit BloomFilterPolicy(int bits_per_key)
		    : bits_per_key_(bits_per_key)
		{
			k_ = static_cast<size_t>(bits_per_key * 0.69); // use k_ = ln2 * bits_pre_key

			if (k_ < 1)
				k_ = 1;
			if (k_ > 30)
				k_ = 30;
		}

		const char* Name() const override { return "prism.BloomFilter"; }

		void CreateFilter(const Slice* keys, int n, std::string* dst) const override
		{
			// compute bloom filter size
			size_t bits = n * bits_per_key_;

			// for small n,  we fix a 64 number
			if (bits < 64)
				bits = 64;

			// get the bytes, and upper bound
			size_t bytes = (bits + 7) / 8;
			bits = bytes * 8;

			// [init_size ... init_size + bytes - 1] : bits array
			// [init_size+bytes] : k_
			const size_t init_size = dst->size();
			dst->resize(init_size + bytes, 0);
			dst->push_back(static_cast<char>(k_)); // remember # hashes

			char* array = &(*dst)[init_size];
			// go through every key
			for (int i = 0; i < n; i++)
			{
				// A hash for the whole key
				uint32_t h = BloomHash(keys[i]);
				const uint32_t delta = (h >> 17) | (h << 15); // Rotate right 17 bits
				for (size_t j = 0; j < k_; j++)
				{
					const uint32_t bitpos = h % bits; // bitpos in [0, bits)
					array[bitpos / 8] |= (1 << (bitpos % 8));
					h += delta;
				}
			}
		}

		bool KeyMayMatch(const Slice& key, const Slice& bloom_filter) const override
		{
			const size_t len = bloom_filter.size();
			if (len < 2) // at least 1byte bits + 1byte k_
				return false;

			const char* array = bloom_filter.data();
			const size_t bits = (len - 1) * 8;

			// use the encoded k_ to get the hash count
			const size_t k = array[len - 1];
			if (k > 30)
			{
				// Reserved for potentially new encodings for short bloom filters.
				// Consider it a match.
				return true;
			}

			uint32_t h = BloomHash(key);
			const uint32_t delta = (h >> 17) | (h << 15); // Rotate right 17 bits
			for (size_t j = 0; j < k; j++)
			{
				const uint32_t bitpos = h % bits;
				if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0) // 0 means false
					return false;
				h += delta;
			}
			return true; // only all 1 return ture
		}

	private:
		size_t bits_per_key_;
		size_t k_;
	};

	const FilterPolicy* NewBloomFilterPolicy(int bits_per_key) { return new BloomFilterPolicy(bits_per_key); }

} // namespace prism
