#ifndef SLICE_H_
#define SLICE_H_

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <span>
#include <string>

namespace prism
{
	class Slice
	{
	public:
		Slice() = default;

		Slice(const char* data, size_t size)
		    : data_(data, size)
		{
		}

		Slice(std::span<const char> data)
		    : data_(data)
		{
		}

		Slice(const std::string& str)
		    : data_(str.data(), str.size())
		{
		}

		Slice(const char* s)
		    : data_(s, std::strlen(s))
		{
		}

		char operator[](size_t n) const
		{
			assert(n < data_.size());
			return data_[n];
		}

		friend bool operator==(const Slice& x, const Slice& y);
		
		void clear() { data_ = std::span<const char>(); }

		void remove_prefix(size_t n)
		{
			assert(n <= data_.size());
			data_ = data_.subspan(n);
		}

		bool starts_with(const Slice& prefix) const
		{
			return data_.size() >= prefix.data_.size()
			    && std::equal(data_.begin(), data_.begin() + prefix.data_.size(), prefix.data_.begin());
		}

		// Three-way comparison.  Returns value:
		//   <  0 iff "*this" <  "b",
		//   == 0 iff "*this" == "b",
		//   >  0 iff "*this" >  "b"
		inline int compare(const Slice& b) const
		{
			const size_t min_len = (data_.size() < b.data_.size()) ? data_.size() : b.data_.size();
			int r = std::memcmp(data_.data(), b.data(), min_len);
			if (r == 0) {
				if (data_.size() < b.data_.size()) r = -1;
				else if (data_.size() > b.data_.size()) r = +1;
			}
			return r;
		}

		const char* begin() const { return data_.data(); }

		const char* end() const { return data_.data() + data_.size(); }

		const char* data() const { return data_.data(); }

		size_t size() const { return data_.size(); }

		bool empty() const { return data_.empty(); }

		std::string ToString() const { return std::string(data_.data(), data_.size()); }

		std::string_view ToStringView() const { return std::string_view(data_.data(), data_.size()); }

	private:
		std::span<const char> data_;
	};

	inline bool operator==(const Slice& x, const Slice& y) { return std::ranges::equal(x.data_, y.data_); }
}

#endif