#ifndef PRISM_FILTER_POLICY_H_
#define PRISM_FILTER_POLICY_H_

#include <string>
#include "slice.h"

namespace prism
{

	class FilterPolicy
	{
	public:
		virtual ~FilterPolicy() = default;

		virtual const char* Name() const = 0;

		virtual void CreateFilter(const Slice* keys, int n, std::string* dst) const = 0;

		virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const = 0;
	};

	const FilterPolicy* NewBloomFilterPolicy(int bits_per_key);

} // namespace prism

#endif