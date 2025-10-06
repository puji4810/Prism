#include "coding.h"

namespace prism
{
	// Use LEB128 encoding for varint32
	// every 7 bits is a byte, the most significant bit is used to indicate if there are more bytes to read
	// if the most significant bit is 1, then there are more bytes to read
	// if the most significant bit is 0, then there are no more bytes to read
	// the value is encoded in the least significant 7 bits of each byte
	// the most significant bit of the last byte is 0
	// the value is encoded in the least significant 7 bits of each byte
	char* EncodeVarint32(char* dst, uint32_t v)
	{
		uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
		static const uint8_t kMore = 0x80;
		if (v < (1u << 7))
		{
			*(ptr++) = static_cast<uint8_t>(v & 0x7f);
		}
		else if (v < (1u << 14))
		{
			*(ptr++) = static_cast<uint8_t>((v & 0x7f) | kMore);
			*(ptr++) = static_cast<uint8_t>((v >> 7) & 0x7f);
		}
		else if (v < (1u << 21))
		{
			*(ptr++) = static_cast<uint8_t>((v & 0x7f) | kMore);
			*(ptr++) = static_cast<uint8_t>(((v >> 7) & 0x7f) | kMore);
			*(ptr++) = static_cast<uint8_t>((v >> 14) & 0x7f);
		}
		else if (v < (1u << 28))
		{
			*(ptr++) = static_cast<uint8_t>((v & 0x7f) | kMore);
			*(ptr++) = static_cast<uint8_t>(((v >> 7) & 0x7f) | kMore);
			*(ptr++) = static_cast<uint8_t>(((v >> 14) & 0x7f) | kMore);
			*(ptr++) = static_cast<uint8_t>((v >> 21) & 0x7f);
		}
		else
		{
			*(ptr++) = static_cast<uint8_t>((v & 0x7f) | kMore);
			*(ptr++) = static_cast<uint8_t>(((v >> 7) & 0x7f) | kMore);
			*(ptr++) = static_cast<uint8_t>(((v >> 14) & 0x7f) | kMore);
			*(ptr++) = static_cast<uint8_t>(((v >> 21) & 0x7f) | kMore);
			*(ptr++) = static_cast<uint8_t>((v >> 28) & 0x7f);
		}
		return reinterpret_cast<char*>(ptr);
	}

	char* EncodeVarint64(char* dst, uint64_t v)
	{
		static const uint8_t kMore = 0x80;
		uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
		while (v >= 128u)
		{
			*(ptr++) = static_cast<uint8_t>((v & 0x7f) | kMore);
			v >>= 7;
		}
		*(ptr++) = static_cast<uint8_t>(v & 0x7f);
		return reinterpret_cast<char*>(ptr);
	}

	void PutFixed32(std::string& dst, uint32_t value)
	{
		char buf[sizeof(value)];
		EncodeFixed32(buf, value);
		dst.append(buf, sizeof(buf));
	}

	void PutFixed64(std::string& dst, uint64_t value)
	{
		char buf[sizeof(value)];
		EncodeFixed64(buf, value);
		dst.append(buf, sizeof(buf));
	}

	void PutVarint32(std::string& dst, uint32_t value)
	{
		char buf[5];
		char* end = EncodeVarint32(buf, value);
		dst.append(buf, static_cast<size_t>(end - buf));
	}

	void PutVarint64(std::string& dst, uint64_t value)
	{
		char buf[10];
		char* end = EncodeVarint64(buf, value);
		dst.append(buf, static_cast<size_t>(end - buf));
	}

	void PutLengthPrefixedSlice(std::string& dst, const Slice& value)
	{
		PutVarint32(dst, value.size());
		dst.append(value.data(), value.size());
	}

	int VarintLength(uint64_t v)
	{
		int len = 1;
		while (v >= 128)
		{
			v >>= 7;
			len++;
		}
		return len;
	}

	bool GetVarint32(Slice& src, uint32_t& value) { return ConsumeVarint32(src, value); }

	bool GetVarint64(Slice& src, uint64_t& value) { return ConsumeVarint64(src, value); }

	bool GetLengthPrefixedSlice(prism::Slice& src, prism::Slice& out)
	{
		uint32_t len = 0;
		if (!ConsumeVarint32(src, len))
			return false;
		if (src.size() < static_cast<size_t>(len))
			return false;
		out = prism::Slice(src.data(), static_cast<size_t>(len));
		src.remove_prefix(static_cast<size_t>(len));
		return true;
	}

	const char* GetVarint32Ptr(const char* p, const char* limit, uint32_t* value)
	{
		uint32_t result = 0;
		int shift = 0;
		while (p < limit && shift <= 28)
		{
			uint8_t byte = static_cast<uint8_t>(*p++);
			result |= static_cast<uint32_t>(byte & 0x7f) << shift;
			if ((byte & 0x80) == 0)
			{
				*value = result;
				return p;
			}
			shift += 7;
		}
		return nullptr;
	}

	bool ConsumeVarint32(Slice& in, uint32_t& v)
	{
		const char* p = in.data();
		const char* limit = p + in.size();
		const char* q = GetVarint32Ptr(p, limit, &v);
		if (!q)
			return false;
		in.remove_prefix(static_cast<size_t>(q - p));
		return true;
	}

	bool ConsumeLengthPrefixedSlice(Slice& in, Slice& out)
	{
		uint32_t len = 0;
		if (!ConsumeVarint32(in, len))
			return false;
		if (in.size() < len)
			return false;
		out = Slice(in.data(), len);
		in.remove_prefix(len);
		return true;
	}

	std::optional<uint32_t> TryDecodeVarint32(Slice in)
	{
		uint32_t v = 0;
		if (ConsumeVarint32(in, v))
			return v;
		return std::nullopt;
	}

	bool ConsumeVarint64(Slice& in, uint64_t& v)
	{
		const char* p = in.data();
		const char* limit = p + in.size();
		const char* q = GetVarint64Ptr(p, limit, &v);
		if (!q)
			return false;
		in.remove_prefix(static_cast<size_t>(q - p));
		return true;
	}

	const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value)
	{
		uint64_t result = 0;
		for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7)
		{
			uint64_t byte = *(reinterpret_cast<const uint8_t*>(p));
			p++;
			if (byte & 128)
			{
				// More bytes are present
				result |= ((byte & 127) << shift);
			}
			else
			{
				result |= (byte << shift);
				*value = result;
				return reinterpret_cast<const char*>(p);
			}
		}
		return nullptr;
	}

	std::optional<uint64_t> TryDecodeVarint64(Slice in)
	{
		uint64_t v = 0;
		if (ConsumeVarint64(in, v))
			return v;
		return std::nullopt;
	}
}