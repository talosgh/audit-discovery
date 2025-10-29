#include "json_utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

char *json_escape_string(const char *input) {
    if (!input) {
        return strdup("");
    }
    size_t capacity = strlen(input) * 2 + 16;
    char *buffer = malloc(capacity);
    if (!buffer) {
        return NULL;
    }
    size_t pos = 0;
    for (const unsigned char *p = (const unsigned char *)input; *p; ++p) {
        unsigned char c = *p;
        if (pos + 6 >= capacity) {
            capacity *= 2;
            char *tmp = realloc(buffer, capacity);
            if (!tmp) {
                free(buffer);
                return NULL;
            }
            buffer = tmp;
        }
        switch (c) {
            case '"':
                buffer[pos++] = '\\';
                buffer[pos++] = '"';
                break;
            case '\\':
                buffer[pos++] = '\\';
                buffer[pos++] = '\\';
                break;
            case '\b':
                buffer[pos++] = '\\';
                buffer[pos++] = 'b';
                break;
            case '\f':
                buffer[pos++] = '\\';
                buffer[pos++] = 'f';
                break;
            case '\n':
                buffer[pos++] = '\\';
                buffer[pos++] = 'n';
                break;
            case '\r':
                buffer[pos++] = '\\';
                buffer[pos++] = 'r';
                break;
            case '\t':
                buffer[pos++] = '\\';
                buffer[pos++] = 't';
                break;
            default:
                if (c < 0x20) {
                    snprintf(buffer + pos, 7, "\\u%04x", c);
                    pos += 6;
                } else {
                    buffer[pos++] = (char)c;
                }
                break;
        }
    }
    buffer[pos] = '\0';
    return buffer;
}
