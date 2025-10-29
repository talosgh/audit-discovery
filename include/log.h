#ifndef LOG_H
#define LOG_H

#include <stdarg.h>

#if defined(__GNUC__) || defined(__clang__)
#define LOG_ATTR_PRINTF(fmt_index, arg_index) __attribute__((format(printf, fmt_index, arg_index)))
#else
#define LOG_ATTR_PRINTF(fmt_index, arg_index)
#endif

void log_error(const char *fmt, ...) LOG_ATTR_PRINTF(1, 2);
void log_info(const char *fmt, ...) LOG_ATTR_PRINTF(1, 2);

#undef LOG_ATTR_PRINTF

#endif /* LOG_H */
