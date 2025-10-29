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

        unsigned int codepoint = 0;
        size_t advance = 0;
        if ((c & 0xE0) == 0xC0 && i + 1 < len) {
            unsigned char c1 = (unsigned char)text[i + 1];
            if ((c1 & 0xC0) == 0x80) {
                codepoint = ((c & 0x1F) << 6) | (c1 & 0x3F);
                advance = 1;
            }
        } else if ((c & 0xF0) == 0xE0 && i + 2 < len) {
            unsigned char c1 = (unsigned char)text[i + 1];
            unsigned char c2 = (unsigned char)text[i + 2];
            if (((c1 & 0xC0) == 0x80) && ((c2 & 0xC0) == 0x80)) {
                codepoint = ((c & 0x0F) << 12) | ((c1 & 0x3F) << 6) | (c2 & 0x3F);
                advance = 2;
            }
        } else if ((c & 0xF8) == 0xF0 && i + 3 < len) {
            unsigned char c1 = (unsigned char)text[i + 1];
            unsigned char c2 = (unsigned char)text[i + 2];
            unsigned char c3 = (unsigned char)text[i + 3];
            if (((c1 & 0xC0) == 0x80) && ((c2 & 0xC0) == 0x80) && ((c3 & 0xC0) == 0x80)) {
                codepoint = ((c & 0x07) << 18) | ((c1 & 0x3F) << 12) | ((c2 & 0x3F) << 6) | (c3 & 0x3F);
                advance = 3;
            }
        }

        if (advance > 0) {
            const char *replacement = NULL;
            char single[4] = {0};
            switch (codepoint) {
                case 0x00A0: single[0] = ' '; replacement = single; break;
                case 0x00B0: replacement = "deg"; break;
                case 0x2018:
                case 0x2019:
                case 0x2032: single[0] = '\''; replacement = single; break;
                case 0x201C:
                case 0x201D:
                case 0x2033: single[0] = '"'; replacement = single; break;
                case 0x2010:
                case 0x2011:
                case 0x2012:
                case 0x2013: single[0] = '-'; replacement = single; break;
                case 0x2014:
                    replacement = "--";
                    break;
                case 0x2022: single[0] = '*'; replacement = single; break;
                case 0x2026: replacement = "..."; break;
                case 0x2122: replacement = "(TM)"; break;
                default:
                    if (codepoint < 0x80 && codepoint >= 0x20) {
                        single[0] = (char)codepoint;
                        replacement = single;
                    }
                    break;
            }
            if (replacement) {
                size_t rep_len = strlen(replacement);
                memcpy(out + o, replacement, rep_len);
                o += rep_len;
            } else {
                out[o++] = '?';
            }
            i += advance;
            continue;
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

        out[o++] = '?';
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
