#ifndef FSUTIL_H
#define FSUTIL_H

#include <stddef.h>

int ensure_directory_exists(const char *path);
char *join_path(const char *dir, const char *filename);
int write_buffer_to_file(const char *path, const char *data, size_t len);

#endif /* FSUTIL_H */
