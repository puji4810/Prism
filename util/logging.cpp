#include "slice.h"
#include "logging.h"

namespace prism
{
	void AppendEscapedStringTo(std::string* str, const Slice& value)
	{
		for (size_t i = 0; i < value.size(); i++)
		{
			char c = value[i];
			if (c >= ' ' && c <= '~')
			{
				str->push_back(c);
			}
			else
			{
				char buf[10];
				std::snprintf(buf, sizeof(buf), "\\x%02x", static_cast<unsigned int>(c) & 0xff);
				str->append(buf);
			}
		}
	}

	std::string EscapeString(const Slice& value)
	{
		std::string r;
		AppendEscapedStringTo(&r, value);
		return r;
	}
}