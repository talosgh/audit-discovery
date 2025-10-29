#include "http.h"

#include "buffer.h"
#include "config.h"
#include "json_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

char *build_success_response(const StringArray *audits) {
    Buffer buf;
    if (!buffer_init(&buf)) {
        return NULL;
    }
    if (!buffer_append_cstr(&buf, "{\"status\":\"ok\",\"audits\":[")) {
        buffer_free(&buf);
        return NULL;
    }
    if (audits && audits->count > 0) {
        for (size_t i = 0; i < audits->count; ++i) {
            if (i > 0 && !buffer_append_char(&buf, ',')) {
                buffer_free(&buf);
                return NULL;
            }
            if (!buffer_append_json_string(&buf, audits->values[i])) {
                buffer_free(&buf);
                return NULL;
            }
        }
    }
    if (!buffer_append_cstr(&buf, "]}")) {
        buffer_free(&buf);
        return NULL;
    }
    char *out = buf.data;
    buf.data = NULL;
    buffer_free(&buf);
    return out;
}

char *build_error_response(const char *message) {
    const char *text = message ? message : "Unknown error";
    char *escaped = json_escape_string(text);
    if (!escaped) {
        return NULL;
    }
    Buffer buf;
    if (!buffer_init(&buf)) {
        free(escaped);
        return NULL;
    }
    bool ok = buffer_append_cstr(&buf, "{\"status\":\"error\",\"message\":\"") &&
              buffer_append_cstr(&buf, escaped) &&
              buffer_append_cstr(&buf, "\"}");
    free(escaped);
    if (!ok) {
        buffer_free(&buf);
        return NULL;
    }
    char *out = buf.data;
    buf.data = NULL;
    buffer_free(&buf);
    return out;
}

void send_http_response(int client_fd, int status_code, const char *status_text, const char *content_type, const void *body, size_t body_len) {
    if (!status_text) {
        status_text = "OK";
    }
    if (!content_type) {
        content_type = "application/json";
    }
    char header[512];
    int header_len = snprintf(header, sizeof(header),
                              "HTTP/1.1 %d %s\r\n"
                              "Content-Type: %s\r\n"
                              "Content-Length: %zu\r\n"
                              "Access-Control-Allow-Origin: *\r\n"
                              "Access-Control-Allow-Methods: GET, POST, PATCH, OPTIONS\r\n"
                              "Access-Control-Allow-Headers: Content-Type, X-API-Key\r\n"
                              "Connection: close\r\n\r\n",
                              status_code, status_text, content_type, body_len);
    if (header_len < 0) {
        return;
    }
    (void)send(client_fd, header, (size_t)header_len, 0);
    if (body_len > 0 && body) {
        (void)send(client_fd, body, body_len, 0);
    }
}

void send_http_json(int client_fd, int status_code, const char *status_text, const char *json_body) {
    size_t len = json_body ? strlen(json_body) : 0;
    send_http_response(client_fd, status_code, status_text, "application/json", json_body, len);
}

const char *mime_type_for(const char *path) {
    const char *ext = strrchr(path, '.');
    if (!ext || ext[1] == '\0') {
        return "text/plain; charset=utf-8";
    }
    ext++;
    if (strcasecmp(ext, "html") == 0) return "text/html; charset=utf-8";
    if (strcasecmp(ext, "css") == 0) return "text/css; charset=utf-8";
    if (strcasecmp(ext, "js") == 0) return "text/javascript; charset=utf-8";
    if (strcasecmp(ext, "mjs") == 0) return "text/javascript; charset=utf-8";
    if (strcasecmp(ext, "json") == 0) return "application/json; charset=utf-8";
    if (strcasecmp(ext, "svg") == 0) return "image/svg+xml";
    if (strcasecmp(ext, "png") == 0) return "image/png";
    if (strcasecmp(ext, "jpg") == 0 || strcasecmp(ext, "jpeg") == 0) return "image/jpeg";
    if (strcasecmp(ext, "webp") == 0) return "image/webp";
    if (strcasecmp(ext, "ico") == 0) return "image/x-icon";
    if (strcasecmp(ext, "txt") == 0) return "text/plain; charset=utf-8";
    if (strcasecmp(ext, "map") == 0) return "application/json; charset=utf-8";
    if (strcasecmp(ext, "woff2") == 0) return "font/woff2";
    return "application/octet-stream";
}

bool path_is_safe(const char *path) {
    if (!path) {
        return false;
    }
    if (strstr(path, "..")) {
        return false;
    }
    if (strchr(path, '\\')) {
        return false;
    }
    return true;
}

static int hex_digit_value(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
    if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
    return -1;
}

static char *url_decode(const char *input) {
    if (!input) return NULL;
    size_t len = strlen(input);
    char *output = malloc(len + 1);
    if (!output) return NULL;
    char *out_ptr = output;
    for (size_t i = 0; i < len; ++i) {
        char c = input[i];
        if (c == '%' && i + 2 < len) {
            int hi = hex_digit_value(input[i + 1]);
            int lo = hex_digit_value(input[i + 2]);
            if (hi >= 0 && lo >= 0) {
                *out_ptr++ = (char)((hi << 4) | lo);
                i += 2;
                continue;
            }
        }
        if (c == '+') {
            *out_ptr++ = ' ';
        } else {
            *out_ptr++ = c;
        }
    }
    *out_ptr = '\0';
    return output;
}

void send_file_download(int client_fd, const char *path, const char *content_type, const char *filename) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        char *body = build_error_response("File not found");
        send_http_json(client_fd, 404, "Not Found", body);
        free(body);
        return;
    }

    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        char *body = build_error_response("Failed to read file");
        send_http_json(client_fd, 500, "Internal Server Error", body);
        free(body);
        return;
    }

    if (!S_ISREG(st.st_mode)) {
        close(fd);
        char *body = build_error_response("Invalid file");
        send_http_json(client_fd, 400, "Bad Request", body);
        free(body);
        return;
    }

    const char *ctype = content_type ? content_type : "application/octet-stream";
    const char *name = filename ? filename : "download.bin";

    char header[1024];
    int header_len = snprintf(header, sizeof(header),
                              "HTTP/1.1 200 OK\r\n"
                              "Content-Type: %s\r\n"
                              "Content-Length: %lld\r\n"
                              "Content-Disposition: attachment; filename=\"%s\"\r\n"
                              "Access-Control-Allow-Origin: *\r\n"
                              "Access-Control-Allow-Methods: GET, POST, PATCH, OPTIONS\r\n"
                              "Access-Control-Allow-Headers: Content-Type, X-API-Key\r\n"
                              "Connection: close\r\n\r\n",
                              ctype,
                              (long long)st.st_size,
                              name);
    if (header_len < 0 || header_len >= (int)sizeof(header)) {
        close(fd);
        char *body = build_error_response("Failed to send headers");
        send_http_json(client_fd, 500, "Internal Server Error", body);
        free(body);
        return;
    }

    if (send(client_fd, header, (size_t)header_len, 0) < 0) {
        close(fd);
        return;
    }

    off_t offset = 0;
    while (offset < st.st_size) {
        ssize_t sent = sendfile(client_fd, fd, &offset, (size_t)(st.st_size - offset));
        if (sent <= 0) {
            if (sent < 0 && errno == EINTR) {
                continue;
            }
            break;
        }
    }

    close(fd);
}

char *http_extract_query_param(const char *query_string, const char *key) {
    if (!query_string || !key || *key == '\0') {
        return NULL;
    }
    char *copy = strdup(query_string);
    if (!copy) {
        return NULL;
    }
    char *token = strtok(copy, "&");
    char *result = NULL;
    while (token) {
        char *eq = strchr(token, '=');
        if (eq) {
            *eq = '\0';
            const char *param_key = token;
            const char *param_value = eq + 1;
            if (strcmp(param_key, key) == 0) {
                char *decoded = url_decode(param_value);
                result = decoded;
                break;
            }
        }
        token = strtok(NULL, "&");
    }
    free(copy);
    return result;
}

void serve_static_file(int client_fd, const char *path) {
    if (!g_static_dir) {
        char *body = build_error_response("Static content unavailable");
        send_http_json(client_fd, 404, "Not Found", body);
        free(body);
        return;
    }

    char relative[PATH_MAX];
    const char *requested = path && *path ? path : "/";
    if (!path_is_safe(requested)) {
        char *body = build_error_response("Invalid path");
        send_http_json(client_fd, 400, "Bad Request", body);
        free(body);
        return;
    }

    const char *effective = requested;
    if (effective[0] == '/') {
        effective++;
    }

    if (*effective == '\0') {
        strncpy(relative, "index.html", sizeof(relative));
    } else {
        strncpy(relative, effective, sizeof(relative));
        relative[sizeof(relative) - 1] = '\0';
    }

    char full_path[PATH_MAX];
    int written = snprintf(full_path, sizeof(full_path), "%s/%s", g_static_dir, relative);
    if (written < 0 || (size_t)written >= sizeof(full_path)) {
        char *body = build_error_response("Path too long");
        send_http_json(client_fd, 414, "Request-URI Too Long", body);
        free(body);
        return;
    }

    struct stat st;
    bool fallback_to_index = false;
    if (stat(full_path, &st) != 0 || S_ISDIR(st.st_mode)) {
        const bool looks_like_asset = strchr(relative, '.') != NULL;
        if (looks_like_asset) {
            char *body = build_error_response("Not Found");
            send_http_json(client_fd, 404, "Not Found", body);
            free(body);
            return;
        }
        fallback_to_index = true;
    }

    if (fallback_to_index) {
        written = snprintf(full_path, sizeof(full_path), "%s/index.html", g_static_dir);
        if (written < 0 || (size_t)written >= sizeof(full_path) || stat(full_path, &st) != 0) {
            char *body = build_error_response("Static index not found");
            send_http_json(client_fd, 404, "Not Found", body);
            free(body);
            return;
        }
    }

    if (!S_ISREG(st.st_mode)) {
        char *body = build_error_response("Not Found");
        send_http_json(client_fd, 404, "Not Found", body);
        free(body);
        return;
    }

    int fd = open(full_path, O_RDONLY);
    if (fd < 0) {
        char *body = build_error_response("Failed to read static asset");
        send_http_json(client_fd, 500, "Internal Server Error", body);
        free(body);
        return;
    }

    const char *content_type = mime_type_for(full_path);
    send_http_response(client_fd, 200, "OK", content_type, NULL, (size_t)st.st_size);

    off_t offset = 0;
    while (offset < st.st_size) {
        ssize_t sent = sendfile(client_fd, fd, &offset, (size_t)(st.st_size - offset));
        if (sent < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        if (sent == 0) {
            break;
        }
    }

    close(fd);
}
