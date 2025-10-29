#include "text_utils.h"

#include "buffer.h"

#include <stdlib.h>
#include <string.h>

static const char *const CP1252_REPLACEMENTS[32] = {
    "EUR",  NULL,   ",",    "f",    "\"",   "...",  "+",    "++",
    "^",    "%",    "S",    "<",    "OE",   NULL,   "Z",    NULL,
    NULL,   "'",    "'",    "\"",   "--",   "*",    "-",    "--",
    "~",    "(TM)", "s",    ">",    "oe",   NULL,   "z",    "Y"
};

char *sanitize_ascii(const char *text) {
    if (!text) {
        return NULL;
    }
    size_t len = strlen(text);
    char *out = malloc(len * 4 + 1);
    if (!out) {
        return NULL;
    }
    size_t o = 0;
    for (size_t i = 0; i < len; ++i) {
        unsigned char c = (unsigned char)text[i];
        if (c == '\r') {
            continue;
        }
        if (c < 0x80) {
            if (c >= 0x20 || c == '\n' || c == '\t') {
                out[o++] = (char)c;
            }
            continue;
        }
        if ((c & 0xE0) == 0xC0 && i + 1 < len) {
            unsigned char c1 = (unsigned char)text[i + 1];
            if ((c1 & 0xC0) == 0x80) {
                out[o++] = (char)c;
                out[o++] = (char)c1;
                i += 1;
                continue;
            }
        }
        if ((c & 0xF0) == 0xE0 && i + 2 < len) {
            unsigned char c1 = (unsigned char)text[i + 1];
            unsigned char c2 = (unsigned char)text[i + 2];
            if (((c1 & 0xC0) == 0x80) && ((c2 & 0xC0) == 0x80)) {
                out[o++] = (char)c;
                out[o++] = (char)c1;
                out[o++] = (char)c2;
                i += 2;
                continue;
            }
        }
        if ((c & 0xF8) == 0xF0 && i + 3 < len) {
            unsigned char c1 = (unsigned char)text[i + 1];
            unsigned char c2 = (unsigned char)text[i + 2];
            unsigned char c3 = (unsigned char)text[i + 3];
            if (((c1 & 0xC0) == 0x80) && ((c2 & 0xC0) == 0x80) && ((c3 & 0xC0) == 0x80)) {
                out[o++] = (char)c;
                out[o++] = (char)c1;
                out[o++] = (char)c2;
                out[o++] = (char)c3;
                i += 3;
                continue;
            }
        }
        if (c >= 0x80 && c <= 0x9F) {
            const char *rep = CP1252_REPLACEMENTS[c - 0x80];
            if (rep) {
                size_t rep_len = strlen(rep);
                memcpy(out + o, rep, rep_len);
                o += rep_len;
            }
            continue;
        }
        if (c >= 0xA0) {
            out[o++] = '?';
        }
    }
    out[o] = '\0';
    char *trimmed = realloc(out, o + 1);
    return trimmed ? trimmed : out;
}

char *latex_escape(const char *text) {
    if (!text) {
        return strdup("");
    }
    Buffer buf;
    if (!buffer_init(&buf)) {
        return NULL;
    }
    for (const unsigned char *ptr = (const unsigned char *)text; *ptr; ++ptr) {
        unsigned char c = *ptr;
        switch (c) {
            case '\\': buffer_append_cstr(&buf, "\\textbackslash{}"); break;
            case '{': buffer_append_cstr(&buf, "\\{"); break;
            case '}': buffer_append_cstr(&buf, "\\}"); break;
            case '#': buffer_append_cstr(&buf, "\\#"); break;
            case '$': buffer_append_cstr(&buf, "\\$"); break;
            case '%': buffer_append_cstr(&buf, "\\%"); break;
            case '&': buffer_append_cstr(&buf, "\\&"); break;
            case '_': buffer_append_cstr(&buf, "\\_"); break;
            case '^': buffer_append_cstr(&buf, "\\textasciicircum{}"); break;
            case '~': buffer_append_cstr(&buf, "\\textasciitilde{}"); break;
            case '\n': buffer_append_cstr(&buf, "\\\\\n"); break;
            default:
                if (c < 0x20) {
                    // Skip control characters
                } else {
                    buffer_append_char(&buf, (char)c);
                }
                break;
        }
    }
    char *escaped = buf.data;
    buf.data = NULL;
    buffer_free(&buf);
    if (!escaped) {
        escaped = strdup("");
    }
    return escaped;
}
