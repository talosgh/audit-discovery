#include "util.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>

void string_array_init(StringArray *array) {
    if (!array) return;
    array->values = NULL;
    array->count = 0;
    array->capacity = 0;
}

int string_array_append_copy(StringArray *array, const char *value) {
    if (!array || !value) {
        return 1;
    }
    if (array->count == array->capacity) {
        size_t new_cap = array->capacity == 0 ? 4 : array->capacity * 2;
        char **tmp = realloc(array->values, new_cap * sizeof(char *));
        if (!tmp) {
            return 0;
        }
        array->values = tmp;
        array->capacity = new_cap;
    }
    array->values[array->count] = strdup(value);
    if (!array->values[array->count]) {
        return 0;
    }
    array->count++;
    return 1;
}

void string_array_clear(StringArray *array) {
    if (!array) {
        return;
    }
    for (size_t i = 0; i < array->count; ++i) {
        free(array->values[i]);
    }
    free(array->values);
    array->values = NULL;
    array->count = 0;
    array->capacity = 0;
}

char *string_array_join(const StringArray *array, const char *separator) {
    if (!array || array->count == 0) {
        return NULL;
    }
    const char *sep = separator ? separator : ", ";
    size_t sep_len = strlen(sep);
    size_t total = 1;
    for (size_t i = 0; i < array->count; ++i) {
        if (array->values[i]) {
            total += strlen(array->values[i]);
        }
        if (i + 1 < array->count) {
            total += sep_len;
        }
    }
    char *result = malloc(total);
    if (!result) {
        return NULL;
    }
    result[0] = '\0';
    for (size_t i = 0; i < array->count; ++i) {
        if (array->values[i]) {
            strcat(result, array->values[i]);
        }
        if (i + 1 < array->count) {
            strcat(result, sep);
        }
    }
    return result;
}

char *trim_copy(const char *input) {
    if (!input) return NULL;
    const char *start = input;
    while (*start && isspace((unsigned char)*start)) {
        start++;
    }
    const char *end = input + strlen(input);
    while (end > start && isspace((unsigned char)*(end - 1))) {
        end--;
    }
    size_t len = (size_t)(end - start);
    char *out = malloc(len + 1);
    if (!out) return NULL;
    memcpy(out, start, len);
    out[len] = '\0';
    return out;
}

bool is_valid_uuid(const char *uuid) {
    if (!uuid) {
        return false;
    }
    size_t len = strlen(uuid);
    if (len != 36) {
        return false;
    }
    for (size_t i = 0; i < len; ++i) {
        char c = uuid[i];
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (c != '-') {
                return false;
            }
        } else if (!isxdigit((unsigned char)c)) {
            return false;
        }
    }
    return true;
}
