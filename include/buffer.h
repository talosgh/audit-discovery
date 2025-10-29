#ifndef BUFFER_H
#define BUFFER_H

#include <stddef.h>

typedef struct {
    char *data;
    size_t length;
    size_t capacity;
} Buffer;

int buffer_init(Buffer *buf);
void buffer_free(Buffer *buf);
int buffer_append_cstr(Buffer *buf, const char *text);
int buffer_append_char(Buffer *buf, char c);
int buffer_appendf(Buffer *buf, const char *fmt, ...);
int buffer_append_json_string(Buffer *buf, const char *text);

#endif /* BUFFER_H */
