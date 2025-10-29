#include "buffer.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int buffer_reserve(Buffer *buf, size_t extra) {
    if (!buf) {
        return 0;
    }
    if (buf->length + extra + 1 <= buf->capacity) {
        return 1;
    }
    size_t new_cap = buf->capacity ? buf->capacity : 1024;
    while (buf->length + extra + 1 > new_cap) {
        new_cap *= 2;
    }
    char *tmp = realloc(buf->data, new_cap);
    if (!tmp) {
        return 0;
    }
    buf->data = tmp;
    buf->capacity = new_cap;
    return 1;
}

static int buffer_append(Buffer *buf, const char *data, size_t len) {
    if (!buf || !data) {
        return 0;
    }
    if (!buffer_reserve(buf, len)) {
        return 0;
    }
    memcpy(buf->data + buf->length, data, len);
    buf->length += len;
    buf->data[buf->length] = '\0';
    return 1;
}

int buffer_init(Buffer *buf) {
    if (!buf) {
        return 0;
    }
    buf->capacity = 1024;
    buf->length = 0;
    buf->data = malloc(buf->capacity);
    if (!buf->data) {
        buf->capacity = 0;
        return 0;
    }
    buf->data[0] = '\0';
    return 1;
}

void buffer_free(Buffer *buf) {
    if (!buf) {
        return;
    }
    free(buf->data);
    buf->data = NULL;
    buf->length = 0;
    buf->capacity = 0;
}

int buffer_append_cstr(Buffer *buf, const char *text) {
    if (!text) {
        return buffer_append(buf, "", 0);
    }
    return buffer_append(buf, text, strlen(text));
}

int buffer_append_char(Buffer *buf, char c) {
    if (!buf) {
        return 0;
    }
    if (!buffer_reserve(buf, 1)) {
        return 0;
    }
    buf->data[buf->length++] = c;
    buf->data[buf->length] = '\0';
    return 1;
}

int buffer_appendf(Buffer *buf, const char *fmt, ...) {
    if (!buf || !fmt) {
        return 0;
    }
    va_list args;
    va_start(args, fmt);
    va_list copy;
    va_copy(copy, args);
    int needed = vsnprintf(NULL, 0, fmt, copy);
    va_end(copy);
    if (needed < 0) {
        va_end(args);
        return 0;
    }
    if (!buffer_reserve(buf, (size_t)needed)) {
        va_end(args);
        return 0;
    }
    vsnprintf(buf->data + buf->length, buf->capacity - buf->length, fmt, args);
    buf->length += (size_t)needed;
    va_end(args);
    return 1;
}

int buffer_append_json_string(Buffer *buf, const char *text) {
    if (!buf) {
        return 0;
    }
    if (!text) {
        return buffer_append_cstr(buf, "null");
    }
    if (!buffer_append_char(buf, '"')) {
        return 0;
    }
    for (const unsigned char *p = (const unsigned char *)text; *p; ++p) {
        unsigned char c = *p;
        switch (c) {
            case '"':
                if (!buffer_append_cstr(buf, "\\\"")) return 0;
                break;
            case '\\':
                if (!buffer_append_cstr(buf, "\\\\")) return 0;
                break;
            case '\b':
                if (!buffer_append_cstr(buf, "\\b")) return 0;
                break;
            case '\f':
                if (!buffer_append_cstr(buf, "\\f")) return 0;
                break;
            case '\n':
                if (!buffer_append_cstr(buf, "\\n")) return 0;
                break;
            case '\r':
                if (!buffer_append_cstr(buf, "\\r")) return 0;
                break;
            case '\t':
                if (!buffer_append_cstr(buf, "\\t")) return 0;
                break;
            default:
                if (c < 0x20) {
                    char esc[7];
                    snprintf(esc, sizeof(esc), "\\u%04x", c);
                    if (!buffer_append_cstr(buf, esc)) return 0;
                } else {
                    if (!buffer_append_char(buf, (char)c)) return 0;
                }
                break;
        }
    }
    return buffer_append_char(buf, '"');
}
