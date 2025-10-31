#include "config.h"

#include "log.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

char *g_api_key = NULL;
char *g_api_prefix = NULL;
size_t g_api_prefix_len = 0;
char *g_static_dir = NULL;
char *g_database_dsn = NULL;
char *g_report_output_dir = NULL;
char *g_report_assets_dir = NULL;
char *g_xai_api_key = NULL;
char *g_google_api_key = NULL;
char *g_google_region_code = NULL;
double g_modernization_cost_per_device = 250000.0;

static void trim_inplace(char *str) {
    if (!str) {
        return;
    }
    size_t start = 0;
    while (str[start] && (str[start] == ' ' || str[start] == '\t')) {
        start++;
    }
    size_t end = strlen(str);
    while (end > start && (str[end - 1] == ' ' || str[end - 1] == '\t')) {
        end--;
    }
    if (start > 0) {
        memmove(str, str + start, end - start);
    }
    str[end - start] = '\0';
}

static void strip_quotes_inplace(char *str) {
    trim_inplace(str);
    size_t len = strlen(str);
    if (len >= 2 && ((str[0] == '"' && str[len - 1] == '"') ||
                     (str[0] == '\'' && str[len - 1] == '\''))) {
        memmove(str, str + 1, len - 2);
        str[len - 2] = '\0';
    }
}

int load_env_file(const char *path) {
    if (!path || path[0] == '\0') {
        return 1;
    }
    FILE *fp = fopen(path, "r");
    if (!fp) {
        if (errno == ENOENT) {
            log_info("Env file %s not found, skipping", path);
            return 1;
        }
        log_error("Failed to open env file %s: %s", path, strerror(errno));
        return 0;
    }

    char line[1024];
    int line_number = 0;
    while (fgets(line, sizeof(line), fp)) {
        line_number++;
        char *newline = strpbrk(line, "\r\n");
        if (newline) {
            *newline = '\0';
        }
        trim_inplace(line);
        if (line[0] == '\0' || line[0] == '#') {
            continue;
        }
        char *eq = strchr(line, '=');
        if (!eq) {
            log_info("Ignoring malformed env line %d in %s", line_number, path);
            continue;
        }
        *eq = '\0';
        char *key = line;
        char *value = eq + 1;
        trim_inplace(key);
        trim_inplace(value);
        strip_quotes_inplace(value);
        if (key[0] == '\0') {
            continue;
        }
        if (setenv(key, value, 1) != 0) {
            log_error("Failed to set env %s from %s: %s", key, path, strerror(errno));
            fclose(fp);
            return 0;
        }
    }
    fclose(fp);
    return 1;
}
