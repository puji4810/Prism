#ifndef PRISM_UTIL_POSIX_LOGGER_H
#define PRISM_UTIL_POSIX_LOGGER_H

#include "env.h"

#include <cstdarg>
#include <cstdio>
#include <mutex>

namespace prism
{

	class PosixLogger final: public Logger
	{
	public:
		explicit PosixLogger(std::FILE* fp);
		~PosixLogger() override;

		void Logv(const char* format, std::va_list ap) override;

	private:
		std::mutex mu_;
		std::FILE* const fp_;
	};

}

#endif

