#ifndef CODING_H_
#define CODING_H_

#include <cstdint>
#include <string>
#include <optional>
#include "slice.h"

namespace prism
{
	// Append the fixed32, fixed64, varint32, varint64, and length prefixed slice to the string
	void PutFixed32(std::string& dst, uint32_t value);
	void PutFixed64(std::string& dst, uint64_t value);
	void PutVarint32(std::string& dst, uint32_t value);
	void PutVarint64(std::string& dst, uint64_t value);
	void PutLengthPrefixedSlice(std::string& dst, const Slice& value);

	bool GetVarint32(Slice* src, uint32_t* value);
	bool GetVarint64(Slice* src, uint64_t* value);
	bool GetLengthPrefixedSlice(Slice* src, Slice* value);

	// Returns the length of the varint32 or varint64 encoding of "v"
	int VarintLength(uint64_t v);

	char* EncodeVarint32(char* dst, uint32_t value);
	char* EncodeVarint64(char* dst, uint64_t value);

	inline static void EncodeFixed32(char* dst, uint32_t value)
	{
		uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);
		buffer[0] = static_cast<uint8_t>(value);
		buffer[1] = static_cast<uint8_t>(value >> 8);
		buffer[2] = static_cast<uint8_t>(value >> 16);
		buffer[3] = static_cast<uint8_t>(value >> 24);
	}

	inline static void EncodeFixed64(char* dst, uint64_t value)
	{
		uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);
		buffer[0] = static_cast<uint8_t>(value);
		buffer[1] = static_cast<uint8_t>(value >> 8);
		buffer[2] = static_cast<uint8_t>(value >> 16);
		buffer[3] = static_cast<uint8_t>(value >> 24);
		buffer[4] = static_cast<uint8_t>(value >> 32);
		buffer[5] = static_cast<uint8_t>(value >> 40);
		buffer[6] = static_cast<uint8_t>(value >> 48);
		buffer[7] = static_cast<uint8_t>(value >> 56);
	}

	inline static uint32_t DecodeFixed32(const char* ptr)
	{
		const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);
		return (static_cast<uint32_t>(buffer[0])) | (static_cast<uint32_t>(buffer[1]) << 8) | (static_cast<uint32_t>(buffer[2]) << 16)
		    | (static_cast<uint32_t>(buffer[3]) << 24);
	}

	inline static uint64_t DecodeFixed64(const char* ptr)
	{
		const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);
		return (static_cast<uint64_t>(buffer[0])) | (static_cast<uint64_t>(buffer[1]) << 8) | (static_cast<uint64_t>(buffer[2]) << 16)
		    | (static_cast<uint64_t>(buffer[3]) << 24) | (static_cast<uint64_t>(buffer[4]) << 32) | (static_cast<uint64_t>(buffer[5]) << 40)
		    | (static_cast<uint64_t>(buffer[6]) << 48) | (static_cast<uint64_t>(buffer[7]) << 56);
	}

	// get the varint32 from the pointer, and advance the pointer
	const char* GetVarint32Ptr(const char* p, const char* limit, uint32_t* value);

	// get the varint64 from the pointer, and advance the pointer
	const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value);

	// consume the varint32 from the slice, and advance the slice
	bool ConsumeVarint32(Slice* in, uint32_t* v);

	// consume the varint64 from the slice, and advance the slice
	bool ConsumeVarint64(Slice* in, uint64_t* v);

	// try to decode the varint32 from the slice, and return the value if successful
	std::optional<uint32_t> TryDecodeVarint32(Slice in);

	// try to decode the varint64 from the slice, and return the value if successful
	std::optional<uint64_t> TryDecodeVarint64(Slice in);

} // namespace prism

#endif // CODING_H_