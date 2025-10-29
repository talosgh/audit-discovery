#ifndef UTIL_H
#define UTIL_H

#include <stddef.h>
#include <stdbool.h>

typedef struct {
    char **values;
    size_t count;
    size_t capacity;
} StringArray;

void string_array_init(StringArray *array);
int string_array_append_copy(StringArray *array, const char *value);
void string_array_clear(StringArray *array);
char *string_array_join(const StringArray *array, const char *separator);
char *trim_copy(const char *input);
bool is_valid_uuid(const char *uuid);

#endif /* UTIL_H */
