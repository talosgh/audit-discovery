#include "log.h"

#include <stdio.h>
#include <stdarg.h>

void log_error(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    fputs("[ERROR] ", stderr);
    vfprintf(stderr, fmt, args);
    fputc('\n', stderr);
    va_end(args);
}

void log_info(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    fputs("[INFO] ", stderr);
    vfprintf(stderr, fmt, args);
    fputc('\n', stderr);
    va_end(args);
}
