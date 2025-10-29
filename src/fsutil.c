#include "fsutil.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

int ensure_directory_exists(const char *path) {
    if (!path || path[0] == '\0') {
        errno = EINVAL;
        return -1;
    }

    char temp[PATH_MAX];
    size_t path_len = strnlen(path, sizeof(temp));
    if (path_len == 0 || path_len >= sizeof(temp)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    memcpy(temp, path, path_len + 1);

    for (size_t i = 1; i < path_len; ++i) {
        if (temp[i] == '/' || temp[i] == '\\') {
            char saved = temp[i];
            temp[i] = '\0';
            if (temp[0] != '\0') {
                if (mkdir(temp, 0775) != 0 && errno != EEXIST) {
                    temp[i] = saved;
                    return -1;
                }
            }
            temp[i] = saved;
        }
    }

    if (mkdir(temp, 0775) != 0 && errno != EEXIST) {
        return -1;
    }

    struct stat st;
    if (stat(temp, &st) != 0) {
        return -1;
    }
    if (!S_ISDIR(st.st_mode)) {
        errno = ENOTDIR;
        return -1;
    }
    return 0;
}

char *join_path(const char *dir, const char *filename) {
    if (!dir || !filename) {
        errno = EINVAL;
        return NULL;
    }
    size_t dir_len = strlen(dir);
    size_t file_len = strlen(filename);
    size_t needs_sep = (dir_len > 0 && dir[dir_len - 1] != '/' && dir[dir_len - 1] != '\\') ? 1 : 0;
    size_t total = dir_len + needs_sep + file_len + 1;
    if (total >= PATH_MAX) {
        errno = ENAMETOOLONG;
        return NULL;
    }
    char *out = malloc(total);
    if (!out) {
        return NULL;
    }
    memcpy(out, dir, dir_len);
    size_t pos = dir_len;
    if (needs_sep) {
        out[pos++] = '/';
    }
    memcpy(out + pos, filename, file_len + 1);
    return out;
}

int write_buffer_to_file(const char *path, const char *data, size_t len) {
    if (!path || !data) {
        errno = EINVAL;
        return -1;
    }
    FILE *fp = fopen(path, "wb");
    if (!fp) {
        return -1;
    }
    size_t written = fwrite(data, 1, len, fp);
    if (written != len) {
        fclose(fp);
        errno = EIO;
        return -1;
    }
    if (fclose(fp) != 0) {
        return -1;
    }
    return 0;
}
