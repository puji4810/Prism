#ifndef PRISM_FILTER_BLOCK_H_
#define PRISM_FILTER_BLOCK_H_

#include "slice.h"
#include "hash.h"
#include <cstddef>
#include <vector>

namespace prism
{
	class FilterPolicy;

	class FilterBlockBuilder
	{
	public:
		explicit FilterBlockBuilder(const FilterPolicy*);

		FilterBlockBuilder(const FilterBlockBuilder&) = delete;
		FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;
		FilterBlockBuilder(FilterBlockBuilder&&) = delete;
		FilterBlockBuilder& operator=(FilterBlockBuilder&&) = delete;

		void StartBlock(uint64_t block_offset);
		void AddKey(const Slice& key);
		Slice Finish();

	private:
		void GenerateFilter();

		const FilterPolicy* policy_;
		std::string keys_;
		std::vector<size_t> start_;
		std::string result_;
		std::vector<Slice> tmp_keys_;
		std::vector<uint32_t> filter_offsets_;
	};

	class FilterBlockReader
	{
	public:
		// REQUIRES: "contents" and *policy must stay live while *this is live.
		explicit FilterBlockReader(const FilterPolicy*, const Slice&);

		bool KeyMayMatch(uint64_t block_offset, const Slice& key);

	private:
		const FilterPolicy* policy_;
		const char* data_; // Pointer to filter data (at block-start)
		const char* offset_; // Pointer to beginning of offset array (at block-end)
		size_t num_; // Number of entries in offset array
		size_t base_lg_; // Encoding parameter (see kFilterBaseLg in .cc file)
	};
}

#endif