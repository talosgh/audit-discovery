#ifndef CSV_H
#define CSV_H

#include <stddef.h>

typedef struct {
    size_t column_count;
    char **values;
} CsvRow;

typedef struct {
    CsvRow header;
    CsvRow *rows;
    size_t row_count;
} CsvFile;

int csv_parse(const char *data, CsvFile *out, char **error_out);
const char *csv_row_get(const CsvFile *file, const CsvRow *row, const char *column_name);
void csv_free(CsvFile *file);

#endif
