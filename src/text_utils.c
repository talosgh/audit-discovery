#include "text_utils.h"

#include "buffer.h"

#include <ctype.h>
#include <stdbool.h>
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

static void latex_append_escaped_char(Buffer *buf, unsigned char c) {
    switch (c) {
        case '\\': buffer_append_cstr(buf, "\\textbackslash{}"); break;
        case '{': buffer_append_cstr(buf, "\\{"); break;
        case '}': buffer_append_cstr(buf, "\\}"); break;
        case '#': buffer_append_cstr(buf, "\\#"); break;
        case '$': buffer_append_cstr(buf, "\\$"); break;
        case '%': buffer_append_cstr(buf, "\\%"); break;
        case '&': buffer_append_cstr(buf, "\\&"); break;
        case '_': buffer_append_cstr(buf, "\\_"); break;
        case '^': buffer_append_cstr(buf, "\\textasciicircum{}"); break;
        case '~': buffer_append_cstr(buf, "\\textasciitilde{}"); break;
        case '\n': buffer_append_cstr(buf, "\\\\\n"); break;
        default:
            if (c < 0x20) {
                // Skip control characters
            } else {
                buffer_append_char(buf, (char)c);
            }
            break;
    }
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
        latex_append_escaped_char(&buf, *ptr);
    }
    char *escaped = buf.data;
    buf.data = NULL;
    buffer_free(&buf);
    if (!escaped) {
        escaped = strdup("");
    }
    return escaped;
}

char *latex_escape_with_markdown(const char *text) {
    if (!text) {
        return strdup("");
    }
    Buffer buf;
    if (!buffer_init(&buf)) {
        return NULL;
    }
    bool bold_open = false;
    for (size_t i = 0; text[i]; ++i) {
        unsigned char c = (unsigned char)text[i];
        if (c == '*' && text[i + 1] == '*') {
            if (bold_open) {
                buffer_append_cstr(&buf, "}");
            } else {
                buffer_append_cstr(&buf, "\\textbf{");
            }
            bold_open = !bold_open;
            ++i;
            continue;
        }
        latex_append_escaped_char(&buf, c);
    }
    if (bold_open) {
        buffer_append_cstr(&buf, "}");
    }
    char *escaped = buf.data;
    buf.data = NULL;
    buffer_free(&buf);
    if (!escaped) {
        escaped = strdup("");
    }
    return escaped;
}

static bool is_all_upper_alpha(const char *text) {
    bool has_alpha = false;
    for (const unsigned char *ptr = (const unsigned char *)text; *ptr; ++ptr) {
        if (isalpha(*ptr)) {
            has_alpha = true;
            if (!isupper(*ptr)) {
                return false;
            }
        }
    }
    return has_alpha;
}

static bool is_acronym_word(const char *text, size_t start, size_t end) {
    static const char *const acronyms[] = {"LLC", "LLP", "INC", "USA", "NYC", "HVAC", "ADA", "DOB"};
    size_t len = end - start;
    if (len == 0) {
        return false;
    }
    if (len <= 3) {
        return true;
    }
    for (size_t i = 0; i < sizeof(acronyms) / sizeof(acronyms[0]); ++i) {
        const char *acro = acronyms[i];
        size_t acro_len = strlen(acro);
        if (acro_len == len) {
            bool match = true;
            for (size_t j = 0; j < len; ++j) {
                unsigned char orig = (unsigned char)text[start + j];
                if (toupper(orig) != (unsigned char)acro[j]) {
                    match = false;
                    break;
                }
            }
            if (match) {
                return true;
            }
        }
    }
    return false;
}

char *normalize_caps_if_all_upper(const char *text) {
    if (!text) {
        return NULL;
    }
    if (!is_all_upper_alpha(text)) {
        char *copy = strdup(text);
        return copy;
    }

    size_t len = strlen(text);
    char *result = malloc(len + 1);
    if (!result) {
        return NULL;
    }

    bool start_word = true;
    for (size_t i = 0; i < len; ++i) {
        unsigned char orig = (unsigned char)text[i];
        if (isalpha(orig)) {
            unsigned char lower = (unsigned char)tolower(orig);
            result[i] = (char)(start_word ? toupper(lower) : lower);
            start_word = false;
        } else {
            result[i] = (char)orig;
            switch (orig) {
                case ' ':
                case '\t':
                case '\n':
                case '-':
                case '/':
                case '(':
                case ')':
                case '\'':
                case '&':
                case '.':
                    start_word = true;
                    break;
                default:
                    start_word = false;
                    break;
            }
        }
    }
    result[len] = '\0';

    size_t pos = 0;
    while (pos < len) {
        while (pos < len && !isalpha((unsigned char)text[pos])) {
            pos++;
        }
        size_t word_start = pos;
        while (pos < len && isalpha((unsigned char)text[pos])) {
            pos++;
        }
        size_t word_end = pos;
        if (word_end > word_start && is_all_upper_alpha(text + word_start)) {
            if (is_acronym_word(text, word_start, word_end)) {
                for (size_t j = word_start; j < word_end; ++j) {
                    result[j] = (char)toupper((unsigned char)result[j]);
                }
            }
        }
    }

    return result;
}

void normalize_caps_inplace(char **text) {
    if (!text || !*text) {
        return;
    }
    char *normalized = normalize_caps_if_all_upper(*text);
    if (!normalized) {
        return;
    }
    if (strcmp(normalized, *text) != 0) {
        free(*text);
        *text = normalized;
    } else {
        free(normalized);
    }
}
