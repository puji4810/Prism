#include "dbformat.h"
#include "coding.h"
#include <sstream>

namespace prism
{
	static uint64_t PackSequenceAndType(uint64_t seq, ValueType t)
	{
		assert(seq <= kMaxSequenceNumber);
		assert(t <= kValueTypeForSeek);
		return (seq << 8) | t;
	}

	size_t InternalKeyEncodingLength(const ParsedInternalKey& key) { return key.user_key.size() + 8; }

	void AppendInternalKey(std::string& result, const ParsedInternalKey& key)
	{
		result.append(key.user_key.data(), key.user_key.size());
		PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
	}

	[[nodiscard]] bool ParseInternalKey(const Slice& internal_key, ParsedInternalKey* result)
	{
		size_t n = internal_key.size();
		if (n < 8)
			return false;
		// skip the user_key, and decode the sequence and type from the last 8 bytes.
		uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
		uint8_t c = static_cast<uint8_t>(num & 0xff);
		result->sequence = num >> 8;
		result->type = static_cast<ValueType>(c);
		result->user_key = Slice(internal_key.data(), n - 8);
		return (c <= static_cast<uint8_t>(kValueTypeForSeek));
	}

	Slice ExtractUserKey(const Slice& internal_key)
	{
		assert(internal_key.size() >= 8);
		return Slice(internal_key.data(), internal_key.size() - 8);
	}

	std::string InternalKey::DebugString() const
	{
		ParsedInternalKey parsed;
		if (ParseInternalKey(rep_, &parsed))
			return parsed.DebugString();
		std::ostringstream ss;
		ss << "(bad)" << rep_;
		return ss.str();
	}

	const char* InternalKeyComparator::Name() const { return "Prism.InternalKeyComparator"; }

	int InternalKeyComparator::Compare(const Slice& a, const Slice& b) const
	{
		// Order by:
		//    increasing user key (according to user-supplied comparator)
		//    decreasing sequence number
		//    decreasing type (though sequence# should be enough to disambiguate)
		int r = user_comparator_->Compare(ExtractUserKey(a), ExtractUserKey(b)); // compare the user key
		if (r == 0)
		{
			// compare the sequence and type
			// docs/dbformat.md: InternalKey comparison follows these rules
			const uint64_t anum = DecodeFixed64(a.data() + a.size() - 8);
			const uint64_t bnum = DecodeFixed64(b.data() + b.size() - 8);
			if (anum > bnum) // newer sequence comes first
				r = -1;
			else if (anum < bnum)
				r = 1;
		}
		return r;
	}

	void InternalKeyComparator::FindShortestSeparator(std::string* start, const Slice& limit) const
	{
		Slice user_start = ExtractUserKey(*start);
		Slice user_limit = ExtractUserKey(limit);
		std::string tmp = user_start.ToString();
		user_comparator_->FindShortestSeparator(&tmp, user_limit);
		if (tmp.size() < user_start.size() && user_comparator_->Compare(user_start, tmp) < 0)
		// ensure the separator is shorter than user_start and strictly less than limit
		{
			// append the sequence and type to the tmp
			PutFixed64(tmp, PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
			assert(Compare(*start, tmp) < 0);
			assert(Compare(tmp, limit) < 0);
			start->swap(tmp);
		}
	}

	void InternalKeyComparator::FindShortSuccessor(std::string* key) const
	{
		Slice user_key = ExtractUserKey(*key);
		std::string tmp = user_key.ToString();
		user_comparator_->FindShortSuccessor(&tmp);
		if (tmp.size() < user_key.size() && user_comparator_->Compare(user_key, tmp) < 0)
		{
			// append the sequence and type to the tmp
			PutFixed64(tmp, PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
			assert(Compare(*key, tmp) < 0);
			key->swap(tmp);
		}
	}

	LookupKey::LookupKey(const Slice& user_key, SequenceNumber sequence)
	{
		size_t usize = user_key.size();
		size_t needed = usize + 13; // A conservative estimate, 5 bytes for the varint32 and 8 bytes for the sequence and type
		char* dst;
		if (needed <= sizeof(space_))
		{
			dst = space_;
		}
		else
		{
			dst = new char[needed];
		}
		start_ = dst;
		dst = EncodeVarint32(dst, usize + 8);
		kstart_ = dst;
		std::memcpy(dst, user_key.data(), usize);
		dst += usize;
		EncodeFixed64(dst, PackSequenceAndType(sequence, kValueTypeForSeek));
		dst += 8;
		end_ = dst;
	}

	LookupKey::~LookupKey()
	{
		if (start_ != space_) // if we dynamically allocated the space, free it
			delete[] start_;
	}
}