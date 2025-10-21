#ifndef PRISM_DBFORMAT_H
#define PRISM_DBFORMAT_H

#include <cstdint>
#include <string>
#include "slice.h"
#include "comparator.h"

namespace prism
{
	enum ValueType : uint8_t
	{
		kTypeDeletion = 0x0,
		kTypeValue = 0x1
	};

	using SequenceNumber = uint64_t;
	constexpr SequenceNumber kMaxSequenceNumber = ((0x1ull << 56) - 1); // left 8 bits for type. they make up of the `tag` together.
	constexpr ValueType kValueTypeForSeek = ValueType::kTypeValue;

	struct InternalKey;
	struct ParsedInternalKey;

	// Return the length of the encoding of "key".
	size_t InternalKeyEncodingLength(const ParsedInternalKey& key);

	// Append the serialization of "key" to *result.
	void AppendInternalKey(std::string& result, const ParsedInternalKey& key);

	// Attempt to parse an internal key from "internal_key".  On success,
	// stores the parsed data in "*result", and returns true.
	//
	// On error, returns false, leaves "*result" in an undefined state.
	[[nodiscard]] bool ParseInternalKey(const Slice& internal_key, ParsedInternalKey* result);

	// Returns the user key portion of an internal key.
	Slice ExtractUserKey(const Slice& internal_key);

	struct ParsedInternalKey
	{
		Slice user_key;
		SequenceNumber sequence;
		ValueType type;

		ParsedInternalKey() = default;

		ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
		    : user_key(u)
		    , sequence(seq)
		    , type(t)
		{
		}

		std::string DebugString() const
		{
			std::string result = "user_key: '";
			result.append(user_key.data(), user_key.size());
			result += "', sequence: ";
			result += std::to_string(sequence);
			result += ", type: ";
			result += (type == ValueType::kTypeValue) ? "Value" : "Deletion";
			return result;
		}
	};

	struct InternalKey
	{
	private:
		std::string rep_;

	public:
		InternalKey() = default;
		InternalKey(const Slice& user_key, SequenceNumber s, ValueType t) { AppendInternalKey(rep_, ParsedInternalKey(user_key, s, t)); }

		bool DecodeFrom(const Slice& s)
		{
			rep_.assign(s.data(), s.size());
			return !rep_.empty();
		}

		Slice Encode() const
		{
			assert(!rep_.empty());
			return rep_;
		}

		Slice user_key() const { return ExtractUserKey(rep_); }

		void SetFrom(const ParsedInternalKey& p)
		{
			rep_.clear();
			AppendInternalKey(rep_, p);
		}

		void Clear() { rep_.clear(); }

		std::string DebugString() const;
	};

	// comparator for internal keys that uses a specified comparator for
	// the user key portion and breaks ties by decreasing sequence number
	// and decreasing type
	struct InternalKeyComparator: public Comparator
	{
	private:
		const Comparator* user_comparator_;

	public:
		explicit InternalKeyComparator(const Comparator* c)
		    : user_comparator_(c)
		{
		}
		const char* Name() const override;
		int Compare(const Slice& a, const Slice& b) const override;
		void FindShortestSeparator(std::string* start, const Slice& limit) const override;
		void FindShortSuccessor(std::string* key) const override;

		const Comparator* user_comparator() const { return user_comparator_; }

		int Compare(const InternalKey& a, const InternalKey& b) const;
	};

	inline int InternalKeyComparator::Compare(const InternalKey& a, const InternalKey& b) const { return Compare(a.Encode(), b.Encode()); }

	// LookupKey for querying MemTable. It encodes the search target.
	// Offer internal_key and user_key for the search target.
	struct LookupKey
	{
	private:
		const char* start_;
		const char* kstart_;
		const char* end_;
		char space_[200]; // Avoid allocation for short keys

	public:
		LookupKey(const Slice& user_key, SequenceNumber sequence);
		LookupKey(const LookupKey&) = delete;
		LookupKey& operator=(const LookupKey&) = delete;
		LookupKey(LookupKey&&) noexcept = default;
		LookupKey& operator=(LookupKey&&) noexcept = default;
		~LookupKey();
		Slice memtable_key() const { return Slice(start_, end_ - start_); }
		Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }
		Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }
	};
}

#endif // PRISM_DBFORMAT_H