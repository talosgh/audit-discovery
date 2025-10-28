#include "json.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#ifndef JSON_MAX_NESTING
#define JSON_MAX_NESTING 256
#endif

typedef struct {
    const char *cur;
    const char *end;
    char *error;
    size_t depth;
} JsonParser;

static void json_set_error(JsonParser *p, const char *message) {
    if (p->error == NULL) {
        p->error = strdup(message);
    }
}

static void json_skip_whitespace(JsonParser *p) {
    while (p->cur < p->end && isspace((unsigned char)*p->cur)) {
        p->cur++;
    }
}

static int json_expect(JsonParser *p, char expected) {
    if (p->cur >= p->end || *p->cur != expected) {
        json_set_error(p, "Unexpected character");
        return 0;
    }
    p->cur++;
    return 1;
}

static char json_peek(JsonParser *p) {
    if (p->cur >= p->end) {
        return '\0';
    }
    return *p->cur;
}

static char json_next(JsonParser *p) {
    if (p->cur >= p->end) {
        return '\0';
    }
    return *p->cur++;
}

static void *json_malloc(size_t size) {
    void *ptr = calloc(1, size);
    return ptr;
}

static char *json_strdup_range(const char *start, size_t len) {
    char *out = malloc(len + 1);
    if (!out) {
        return NULL;
    }
    memcpy(out, start, len);
    out[len] = '\0';
    return out;
}

static int json_hex_value(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return 10 + (c - 'a');
    }
    if (c >= 'A' && c <= 'F') {
        return 10 + (c - 'A');
    }
    return -1;
}

static void json_append_utf8(char **buffer, size_t *length, size_t *capacity, unsigned codepoint) {
    char utf8[4];
    size_t count = 0;
    if (codepoint <= 0x7F) {
        utf8[count++] = (char)codepoint;
    } else if (codepoint <= 0x7FF) {
        utf8[count++] = (char)(0xC0 | ((codepoint >> 6) & 0x1F));
        utf8[count++] = (char)(0x80 | (codepoint & 0x3F));
    } else {
        utf8[count++] = (char)(0xE0 | ((codepoint >> 12) & 0x0F));
        utf8[count++] = (char)(0x80 | ((codepoint >> 6) & 0x3F));
        utf8[count++] = (char)(0x80 | (codepoint & 0x3F));
    }
    if (*length + count + 1 > *capacity) {
        size_t new_cap = (*capacity == 0 ? 32 : *capacity * 2);
        while (*length + count + 1 > new_cap) {
            new_cap *= 2;
        }
        char *tmp = realloc(*buffer, new_cap);
        if (!tmp) {
            return;
        }
        *buffer = tmp;
        *capacity = new_cap;
    }
    memcpy(*buffer + *length, utf8, count);
    *length += count;
}

static char *json_parse_string_value(JsonParser *p) {
    if (json_next(p) != '"') {
        json_set_error(p, "Expected string opening quote");
        return NULL;
    }
    char *buffer = NULL;
    size_t length = 0;
    size_t capacity = 0;
    while (p->cur < p->end) {
        char c = json_next(p);
        if (c == '\0') {
            break;
        }
        if (c == '"') {
            break;
        }
        if (c == '\\') {
            char esc = json_next(p);
            if (esc == '"' || esc == '\\' || esc == '/') {
                json_append_utf8(&buffer, &length, &capacity, (unsigned char)esc);
            } else if (esc == 'b') {
                json_append_utf8(&buffer, &length, &capacity, '\b');
            } else if (esc == 'f') {
                json_append_utf8(&buffer, &length, &capacity, '\f');
            } else if (esc == 'n') {
                json_append_utf8(&buffer, &length, &capacity, '\n');
            } else if (esc == 'r') {
                json_append_utf8(&buffer, &length, &capacity, '\r');
            } else if (esc == 't') {
                json_append_utf8(&buffer, &length, &capacity, '\t');
            } else if (esc == 'u') {
                int v1 = json_hex_value(json_next(p));
                int v2 = json_hex_value(json_next(p));
                int v3 = json_hex_value(json_next(p));
                int v4 = json_hex_value(json_next(p));
                if (v1 < 0 || v2 < 0 || v3 < 0 || v4 < 0) {
                    json_set_error(p, "Invalid unicode escape");
                    free(buffer);
                    return NULL;
                }
                unsigned code = (unsigned)((v1 << 12) | (v2 << 8) | (v3 << 4) | v4);
                json_append_utf8(&buffer, &length, &capacity, code);
            } else {
                json_set_error(p, "Unknown escape sequence");
                free(buffer);
                return NULL;
            }
        } else {
            json_append_utf8(&buffer, &length, &capacity, (unsigned char)c);
        }
    }
    if (buffer == NULL) {
        buffer = malloc(1);
        if (!buffer) {
            return NULL;
        }
    }
    buffer[length] = '\0';
    return buffer;
}

static JsonValue *json_new_value(JsonType type) {
    JsonValue *value = json_malloc(sizeof(JsonValue));
    if (!value) {
        return NULL;
    }
    value->type = type;
    if (type == JSON_ARRAY) {
        value->value.array.items = NULL;
        value->value.array.count = 0;
    } else if (type == JSON_OBJECT) {
        value->value.object.keys = NULL;
        value->value.object.values = NULL;
        value->value.object.count = 0;
    }
    return value;
}

static JsonValue *json_parse_value(JsonParser *p);

static JsonValue *json_parse_array(JsonParser *p) {
    if (!json_expect(p, '[')) {
        return NULL;
    }
    JsonValue *array = json_new_value(JSON_ARRAY);
    if (!array) {
        return NULL;
    }
    json_skip_whitespace(p);
    if (json_peek(p) == ']') {
        json_next(p);
        return array;
    }
    while (p->cur < p->end) {
        if (p->depth >= JSON_MAX_NESTING) {
            json_set_error(p, "JSON is too deeply nested");
            json_free(array);
            return NULL;
        }
        p->depth++;
        JsonValue *item = json_parse_value(p);
        p->depth--;
        if (!item) {
            json_free(array);
            return NULL;
        }
        size_t new_count = array->value.array.count + 1;
        JsonValue **tmp = realloc(array->value.array.items, new_count * sizeof(JsonValue *));
        if (!tmp) {
            json_free(item);
            json_free(array);
            return NULL;
        }
        array->value.array.items = tmp;
        array->value.array.items[array->value.array.count] = item;
        array->value.array.count = new_count;
        json_skip_whitespace(p);
        char c = json_peek(p);
        if (c == ',') {
            json_next(p);
            json_skip_whitespace(p);
            continue;
        }
        if (c == ']') {
            json_next(p);
            break;
        }
        json_set_error(p, "Expected ',' or ']' in array");
        json_free(array);
        return NULL;
    }
    return array;
}

static JsonValue *json_parse_object(JsonParser *p) {
    if (!json_expect(p, '{')) {
        return NULL;
    }
    JsonValue *object = json_new_value(JSON_OBJECT);
    if (!object) {
        return NULL;
    }
    json_skip_whitespace(p);
    if (json_peek(p) == '}') {
        json_next(p);
        return object;
    }
    while (p->cur < p->end) {
        json_skip_whitespace(p);
        if (json_peek(p) != '"') {
            json_set_error(p, "Expected string key in object");
            json_free(object);
            return NULL;
        }
        char *key = json_parse_string_value(p);
        if (!key) {
            json_free(object);
            return NULL;
        }
        json_skip_whitespace(p);
        if (!json_expect(p, ':')) {
            free(key);
            json_free(object);
            return NULL;
        }
        json_skip_whitespace(p);
        if (p->depth >= JSON_MAX_NESTING) {
            json_set_error(p, "JSON is too deeply nested");
            free(key);
            json_free(object);
            return NULL;
        }
        p->depth++;
        JsonValue *val = json_parse_value(p);
        p->depth--;
        if (!val) {
            free(key);
            json_free(object);
            return NULL;
        }
        size_t new_count = object->value.object.count + 1;
        char **new_keys = realloc(object->value.object.keys, new_count * sizeof(char *));
        JsonValue **new_values = realloc(object->value.object.values, new_count * sizeof(JsonValue *));
        if (!new_keys || !new_values) {
            free(key);
            json_free(val);
            if (new_keys) object->value.object.keys = new_keys;
            if (new_values) object->value.object.values = new_values;
            json_free(object);
            return NULL;
        }
        object->value.object.keys = new_keys;
        object->value.object.values = new_values;
        object->value.object.keys[object->value.object.count] = key;
        object->value.object.values[object->value.object.count] = val;
        object->value.object.count = new_count;
        json_skip_whitespace(p);
        char c = json_peek(p);
        if (c == ',') {
            json_next(p);
            json_skip_whitespace(p);
            continue;
        }
        if (c == '}') {
            json_next(p);
            break;
        }
        json_set_error(p, "Expected ',' or '}' in object");
        json_free(object);
        return NULL;
    }
    return object;
}

static JsonValue *json_parse_number(JsonParser *p) {
    const char *start = p->cur;
    while (p->cur < p->end && strchr("-+0123456789.eE", *p->cur)) {
        p->cur++;
    }
    size_t len = (size_t)(p->cur - start);
    char *copy = json_strdup_range(start, len);
    if (!copy) {
        return NULL;
    }
    char *endptr = NULL;
    double val = strtod(copy, &endptr);
    if (endptr == copy) {
        json_set_error(p, "Invalid number");
        free(copy);
        return NULL;
    }
    free(copy);
    JsonValue *number = json_new_value(JSON_NUMBER);
    if (!number) {
        return NULL;
    }
    number->value.number = val;
    return number;
}

static int json_match_literal(JsonParser *p, const char *literal) {
    size_t len = strlen(literal);
    if ((size_t)(p->end - p->cur) < len) {
        return 0;
    }
    if (strncmp(p->cur, literal, len) == 0) {
        p->cur += len;
        return 1;
    }
    return 0;
}

static JsonValue *json_parse_value(JsonParser *p) {
    json_skip_whitespace(p);
    if (p->cur >= p->end) {
        json_set_error(p, "Unexpected end of JSON");
        return NULL;
    }
    char c = *p->cur;
    if (c == '"') {
        char *str = json_parse_string_value(p);
        if (!str) {
            return NULL;
        }
        JsonValue *value = json_new_value(JSON_STRING);
        if (!value) {
            free(str);
            return NULL;
        }
        value->value.string = str;
        return value;
    }
    if (c == '{') {
        return json_parse_object(p);
    }
    if (c == '[') {
        return json_parse_array(p);
    }
    if (c == 't') {
        if (!json_match_literal(p, "true")) {
            json_set_error(p, "Invalid literal");
            return NULL;
        }
        JsonValue *value = json_new_value(JSON_BOOL);
        if (!value) {
            return NULL;
        }
        value->value.boolean = true;
        return value;
    }
    if (c == 'f') {
        if (!json_match_literal(p, "false")) {
            json_set_error(p, "Invalid literal");
            return NULL;
        }
        JsonValue *value = json_new_value(JSON_BOOL);
        if (!value) {
            return NULL;
        }
        value->value.boolean = false;
        return value;
    }
    if (c == 'n') {
        if (!json_match_literal(p, "null")) {
            json_set_error(p, "Invalid literal");
            return NULL;
        }
        JsonValue *value = json_new_value(JSON_NULL);
        return value;
    }
    if (c == '-' || c == '+' || (c >= '0' && c <= '9')) {
        return json_parse_number(p);
    }
    json_set_error(p, "Unexpected character");
    return NULL;
}

JsonValue *json_parse(const char *text, char **error_out) {
    if (!text) {
        if (error_out) {
            *error_out = strdup("Empty JSON");
        }
        return NULL;
    }
    JsonParser parser;
    parser.cur = text;
    parser.end = text + strlen(text);
    parser.error = NULL;
    parser.depth = 0;
    JsonValue *value = json_parse_value(&parser);
    if (!value) {
        if (error_out) {
            *error_out = parser.error ? parser.error : strdup("Failed to parse JSON");
        } else {
            free(parser.error);
        }
        return NULL;
    }
    json_skip_whitespace(&parser);
    if (parser.cur != parser.end) {
        json_set_error(&parser, "Trailing characters after JSON document");
    }
    if (parser.error) {
        if (error_out) {
            *error_out = parser.error;
        } else {
            free(parser.error);
        }
        json_free(value);
        return NULL;
    }
    if (error_out) {
        *error_out = NULL;
    }
    return value;
}

void json_free(JsonValue *value) {
    if (!value) {
        return;
    }
    switch (value->type) {
        case JSON_STRING:
            free(value->value.string);
            break;
        case JSON_ARRAY:
            for (size_t i = 0; i < value->value.array.count; ++i) {
                json_free(value->value.array.items[i]);
            }
            free(value->value.array.items);
            break;
        case JSON_OBJECT:
            for (size_t i = 0; i < value->value.object.count; ++i) {
                free(value->value.object.keys[i]);
                json_free(value->value.object.values[i]);
            }
            free(value->value.object.keys);
            free(value->value.object.values);
            break;
        case JSON_NUMBER:
        case JSON_BOOL:
        case JSON_NULL:
        default:
            break;
    }
    free(value);
}

static int json_object_find_index(const JsonValue *object, const char *key) {
    if (!object || object->type != JSON_OBJECT || !key) {
        return -1;
    }
    for (size_t i = 0; i < object->value.object.count; ++i) {
        if (strcmp(object->value.object.keys[i], key) == 0) {
            return (int)i;
        }
    }
    return -1;
}

JsonValue *json_object_get(const JsonValue *object, const char *key) {
    int idx = json_object_find_index(object, key);
    if (idx < 0) {
        return NULL;
    }
    return object->value.object.values[idx];
}

JsonValue *json_array_get(const JsonValue *array, size_t index) {
    if (!array || array->type != JSON_ARRAY) {
        return NULL;
    }
    if (index >= array->value.array.count) {
        return NULL;
    }
    return array->value.array.items[index];
}

size_t json_array_size(const JsonValue *array) {
    if (!array || array->type != JSON_ARRAY) {
        return 0;
    }
    return array->value.array.count;
}

JsonValue *json_object_get_path(const JsonValue *object, const char *path) {
    if (!object || object->type != JSON_OBJECT || !path) {
        return NULL;
    }
    char *copy = strdup(path);
    if (!copy) {
        return NULL;
    }
    JsonValue *current = (JsonValue *)object;
    char *segment = strtok(copy, ".");
    while (segment && current) {
        char *bracket = strchr(segment, '[');
        char *array_index_str = NULL;
        int array_index = -1;
        if (bracket) {
            *bracket = '\0';
            array_index_str = bracket + 1;
            char *closing = strchr(array_index_str, ']');
            if (closing) {
                *closing = '\0';
                array_index = atoi(array_index_str);
            }
        }
        if (*segment != '\0') {
            current = json_object_get(current, segment);
        }
        if (!current) {
            break;
        }
        if (array_index >= 0) {
            current = json_array_get(current, (size_t)array_index);
        }
        segment = strtok(NULL, ".");
    }
    free(copy);
    return current;
}

const char *json_as_string(const JsonValue *value) {
    if (!value || value->type != JSON_STRING) {
        return NULL;
    }
    return value->value.string;
}

int json_as_int(const JsonValue *value, int *out) {
    if (!value) {
        return 0;
    }
    if (value->type == JSON_NUMBER) {
        if (out) {
            *out = (int)value->value.number;
        }
        return 1;
    }
    if (value->type == JSON_STRING) {
        char *endptr = NULL;
        long val = strtol(value->value.string, &endptr, 10);
        if (endptr && *endptr == '\0') {
            if (out) {
                *out = (int)val;
            }
            return 1;
        }
    }
    return 0;
}

int json_as_long(const JsonValue *value, long *out) {
    if (!value) {
        return 0;
    }
    if (value->type == JSON_NUMBER) {
        if (out) {
            *out = (long)value->value.number;
        }
        return 1;
    }
    if (value->type == JSON_STRING) {
        char *endptr = NULL;
        long val = strtol(value->value.string, &endptr, 10);
        if (endptr && *endptr == '\0') {
            if (out) {
                *out = val;
            }
            return 1;
        }
    }
    return 0;
}

double json_as_double_default(const JsonValue *value, double default_value) {
    if (!value) {
        return default_value;
    }
    if (value->type == JSON_NUMBER) {
        return value->value.number;
    }
    if (value->type == JSON_STRING) {
        char *endptr = NULL;
        double val = strtod(value->value.string, &endptr);
        if (endptr && *endptr == '\0') {
            return val;
        }
    }
    return default_value;
}

bool json_as_bool_default(const JsonValue *value, bool default_value) {
    if (!value) {
        return default_value;
    }
    if (value->type == JSON_BOOL) {
        return value->value.boolean;
    }
    if (value->type == JSON_NUMBER) {
        return value->value.number != 0.0;
    }
    if (value->type == JSON_STRING) {
        if (strcmp(value->value.string, "1") == 0 || strcasecmp(value->value.string, "true") == 0 || strcasecmp(value->value.string, "t") == 0) {
            return true;
        }
        if (strcmp(value->value.string, "0") == 0 || strcasecmp(value->value.string, "false") == 0 || strcasecmp(value->value.string, "f") == 0) {
            return false;
        }
    }
    return default_value;
}
