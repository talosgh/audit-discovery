#ifndef JSON_H
#define JSON_H

#include <stddef.h>
#include <stdbool.h>

typedef enum {
    JSON_NULL,
    JSON_BOOL,
    JSON_NUMBER,
    JSON_STRING,
    JSON_ARRAY,
    JSON_OBJECT
} JsonType;

typedef struct JsonValue JsonValue;

struct JsonValue {
    JsonType type;
    union {
        bool boolean;
        double number;
        char *string;
        struct {
            JsonValue **items;
            size_t count;
        } array;
        struct {
            char **keys;
            JsonValue **values;
            size_t count;
        } object;
    } value;
};

JsonValue *json_parse(const char *text, char **error_out);
void json_free(JsonValue *value);

JsonValue *json_object_get(const JsonValue *object, const char *key);
JsonValue *json_object_get_path(const JsonValue *object, const char *path);
JsonValue *json_array_get(const JsonValue *array, size_t index);
size_t json_array_size(const JsonValue *array);

const char *json_as_string(const JsonValue *value);
int json_as_int(const JsonValue *value, int *out);
int json_as_long(const JsonValue *value, long *out);
double json_as_double_default(const JsonValue *value, double default_value);
bool json_as_bool_default(const JsonValue *value, bool default_value);

#endif
