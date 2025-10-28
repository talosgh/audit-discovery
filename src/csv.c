#include "csv.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char **data;
    size_t count;
    size_t capacity;
} StringList;

static void string_list_init(StringList *list) {
    list->data = NULL;
    list->count = 0;
    list->capacity = 0;
}

static int string_list_append(StringList *list, char *value) {
    if (list->count == list->capacity) {
        size_t new_cap = list->capacity == 0 ? 8 : list->capacity * 2;
        char **tmp = realloc(list->data, new_cap * sizeof(char *));
        if (!tmp) {
            return 0;
        }
        list->data = tmp;
        list->capacity = new_cap;
    }
    list->data[list->count++] = value;
    return 1;
}

static void string_list_free(StringList *list) {
    for (size_t i = 0; i < list->count; ++i) {
        free(list->data[i]);
    }
    free(list->data);
    list->data = NULL;
    list->count = 0;
    list->capacity = 0;
}

static void skip_line_breaks(const char **cursor) {
    if (**cursor == '\r') {
        (*cursor)++;
    }
    if (**cursor == '\n') {
        (*cursor)++;
    }
}

static char *parse_csv_field(const char **cursor, char **error_out) {
    const char *ptr = *cursor;
    int quoted = 0;
    if (*ptr == '"') {
        quoted = 1;
        ptr++;
    }
    size_t capacity = 64;
    size_t length = 0;
    char *buffer = malloc(capacity);
    if (!buffer) {
        if (error_out) {
            *error_out = strdup("Out of memory while parsing CSV");
        }
        return NULL;
    }
    while (*ptr) {
        char c = *ptr;
        if (quoted) {
            if (c == '"') {
                if (*(ptr + 1) == '"') {
                    if (length + 1 >= capacity) {
                        capacity *= 2;
                        char *tmp = realloc(buffer, capacity);
                        if (!tmp) {
                            if (error_out) {
                                *error_out = strdup("Out of memory while parsing CSV");
                            }
                            free(buffer);
                            return NULL;
                        }
                        buffer = tmp;
                    }
                    buffer[length++] = '"';
                    ptr += 2;
                    continue;
                }
                ptr++;
                quoted = 0;
                break;
            }
            if (length + 1 >= capacity) {
                capacity *= 2;
                char *tmp = realloc(buffer, capacity);
                if (!tmp) {
                    if (error_out) {
                        *error_out = strdup("Out of memory while parsing CSV");
                    }
                    free(buffer);
                    return NULL;
                }
                buffer = tmp;
            }
            buffer[length++] = c;
            ptr++;
            continue;
        }
        if (c == ',' || c == '\r' || c == '\n' || c == '\0') {
            break;
        }
        if (length + 1 >= capacity) {
            capacity *= 2;
            char *tmp = realloc(buffer, capacity);
            if (!tmp) {
                if (error_out) {
                    *error_out = strdup("Out of memory while parsing CSV");
                }
                free(buffer);
                return NULL;
            }
            buffer = tmp;
        }
        buffer[length++] = c;
        ptr++;
    }
    if (quoted) {
        if (error_out) {
            *error_out = strdup("Unterminated quoted field in CSV");
        }
        free(buffer);
        return NULL;
    }
    buffer[length] = '\0';
    *cursor = ptr;
    return buffer;
}

static int parse_csv_row(const char **cursor, StringList *fields, char **error_out) {
    string_list_init(fields);
    while (**cursor && **cursor != '\n' && **cursor != '\r') {
        char *field = parse_csv_field(cursor, error_out);
        if (!field) {
            string_list_free(fields);
            return 0;
        }
        if (!string_list_append(fields, field)) {
            if (error_out) {
                *error_out = strdup("Out of memory while parsing CSV");
            }
            free(field);
            string_list_free(fields);
            return 0;
        }
        if (**cursor == ',') {
            (*cursor)++;
            continue;
        }
        break;
    }
    if (**cursor == '\r' || **cursor == '\n') {
        skip_line_breaks(cursor);
    }
    return 1;
}

static int convert_string_list_to_row(StringList *list, CsvRow *row) {
    row->column_count = list->count;
    row->values = malloc(list->count * sizeof(char *));
    if (!row->values) {
        return 0;
    }
    for (size_t i = 0; i < list->count; ++i) {
        row->values[i] = list->data[i];
    }
    list->count = 0;
    free(list->data);
    list->data = NULL;
    list->capacity = 0;
    return 1;
}

int csv_parse(const char *data, CsvFile *out, char **error_out) {
    if (!data || !out) {
        if (error_out) {
            *error_out = strdup("Invalid CSV input");
        }
        return 0;
    }
    memset(out, 0, sizeof(*out));
    const char *cursor = data;
    StringList header_list;
    if (!parse_csv_row(&cursor, &header_list, error_out)) {
        return 0;
    }
    if (!convert_string_list_to_row(&header_list, &out->header)) {
        if (error_out) {
            *error_out = strdup("Out of memory while storing CSV header");
        }
        string_list_free(&header_list);
        return 0;
    }
    out->rows = NULL;
    out->row_count = 0;
    size_t capacity = 0;
    while (*cursor) {
        StringList row_fields;
        const char *row_start = cursor;
        if (*cursor == '\0') {
            break;
        }
        if (*cursor == '\r' || *cursor == '\n') {
            skip_line_breaks(&cursor);
            continue;
        }
        if (!parse_csv_row(&cursor, &row_fields, error_out)) {
            csv_free(out);
            return 0;
        }
        if (row_fields.count == 0) {
            string_list_free(&row_fields);
            continue;
        }
        CsvRow row;
        if (!convert_string_list_to_row(&row_fields, &row)) {
            if (error_out) {
                *error_out = strdup("Out of memory while storing CSV row");
            }
            string_list_free(&row_fields);
            csv_free(out);
            return 0;
        }
        if (out->row_count == capacity) {
            size_t new_cap = capacity == 0 ? 8 : capacity * 2;
            CsvRow *tmp = realloc(out->rows, new_cap * sizeof(CsvRow));
            if (!tmp) {
                if (error_out) {
                    *error_out = strdup("Out of memory while expanding CSV rows");
                }
                for (size_t i = 0; i < row.column_count; ++i) {
                    free(row.values[i]);
                }
                free(row.values);
                csv_free(out);
                return 0;
            }
            out->rows = tmp;
            capacity = new_cap;
        }
        if (row.column_count != out->header.column_count) {
            if (error_out) {
                *error_out = strdup("CSV row column count mismatch");
            }
            for (size_t i = 0; i < row.column_count; ++i) {
                free(row.values[i]);
            }
            free(row.values);
            csv_free(out);
            return 0;
        }
        out->rows[out->row_count++] = row;
        (void)row_start;
    }
    return 1;
}

const char *csv_row_get(const CsvFile *file, const CsvRow *row, const char *column_name) {
    if (!file || !row || !column_name) {
        return NULL;
    }
    for (size_t i = 0; i < file->header.column_count; ++i) {
        if (strcmp(file->header.values[i], column_name) == 0) {
            return row->values[i];
        }
    }
    return NULL;
}

void csv_free(CsvFile *file) {
    if (!file) {
        return;
    }
    for (size_t i = 0; i < file->header.column_count; ++i) {
        free(file->header.values[i]);
    }
    free(file->header.values);
    file->header.values = NULL;
    file->header.column_count = 0;
    for (size_t r = 0; r < file->row_count; ++r) {
        CsvRow *row = &file->rows[r];
        for (size_t i = 0; i < row->column_count; ++i) {
            free(row->values[i]);
        }
        free(row->values);
    }
    free(file->rows);
    file->rows = NULL;
    file->row_count = 0;
}

