#include "routes.h"

#include "buffer.h"
#include "db_helpers.h"
#include "http.h"
#include "json.h"
#include "log.h"
#include "util.h"

#include <stdlib.h>
#include <string.h>
#include <strings.h>

void routes_handle_get(int client_fd, PGconn *conn, const char *path, const char *query_string) {
    (void)client_fd;
    (void)conn;
    (void)path;
    (void)query_string;
    // Temporary stub; main currently implements GET routing directly.
}

static bool extract_resolved_flag(const char *body, bool *resolved_out) {
    if (!body || !resolved_out) {
        return false;
    }

    char *parse_error = NULL;
    JsonValue *root = json_parse(body, &parse_error);
    if (root) {
        bool found = false;
        if (root->type == JSON_OBJECT) {
            JsonValue *resolved_val = json_object_get(root, "resolved");
            if (resolved_val) {
                if (resolved_val->type == JSON_BOOL) {
                    *resolved_out = resolved_val->value.boolean;
                    found = true;
                } else if (resolved_val->type == JSON_STRING && resolved_val->value.string) {
                    const char *val = resolved_val->value.string;
                    if (strcasecmp(val, "true") == 0 || strcmp(val, "1") == 0) {
                        *resolved_out = true;
                        found = true;
                    } else if (strcasecmp(val, "false") == 0 || strcmp(val, "0") == 0) {
                        *resolved_out = false;
                        found = true;
                    }
                } else if (resolved_val->type == JSON_NUMBER) {
                    *resolved_out = resolved_val->value.number != 0.0;
                    found = true;
                }
            }
        }
        json_free(root);
        if (parse_error) free(parse_error);
        return found;
    }
    if (parse_error) {
        free(parse_error);
    }

    const char *needle = "\"resolved\"";
    const char *pos = body;
    while ((pos = strstr(pos, needle)) != NULL) {
        const char *after = pos + strlen(needle);
        while (*after == ' ' || *after == '\t' || *after == '\r' || *after == '\n') {
            after++;
        }
        if (*after != ':') {
            pos = after;
            continue;
        }
        after++;
        while (*after == ' ' || *after == '\t' || *after == '\r' || *after == '\n') {
            after++;
        }
        if (strncasecmp(after, "true", 4) == 0) {
            *resolved_out = true;
            return true;
        }
        if (strncasecmp(after, "false", 5) == 0) {
            *resolved_out = false;
            return true;
        }
        if (*after == '"') {
            after++;
            if (strncasecmp(after, "true\"", 5) == 0) {
                *resolved_out = true;
                return true;
            }
            if (strncasecmp(after, "false\"", 6) == 0) {
                *resolved_out = false;
                return true;
            }
        }
        if (*after == '0' || *after == '1') {
            *resolved_out = (*after != '0');
            return true;
        }
        pos = after;
    }
    return false;
}

bool routes_handle_patch(int client_fd, PGconn *conn, const char *api_path, const char *body_json) {
    if (!api_path || strncmp(api_path, "/audits/", 8) != 0) {
        return false;
    }

    const char *rest = api_path + 8;
    const char *slash = strchr(rest, '/');
    if (!slash) {
        char *body = build_error_response("Invalid deficiency path");
        send_http_json(client_fd, 400, "Bad Request", body);
        free(body);
        return true;
    }
    size_t uuid_len = (size_t)(slash - rest);
    if (uuid_len == 0 || uuid_len >= 64) {
        char *body = build_error_response("Invalid audit id");
        send_http_json(client_fd, 400, "Bad Request", body);
        free(body);
        return true;
    }
    char target_uuid[64];
    memcpy(target_uuid, rest, uuid_len);
    target_uuid[uuid_len] = '\0';
    if (!is_valid_uuid(target_uuid)) {
        char *body = build_error_response("Invalid audit id");
        send_http_json(client_fd, 400, "Bad Request", body);
        free(body);
        return true;
    }

    const char *def_path = slash + 1;
    if (strncmp(def_path, "deficiencies/", 13) != 0) {
        char *body = build_error_response("Invalid deficiency path");
        send_http_json(client_fd, 400, "Bad Request", body);
        free(body);
        return true;
    }
    const char *id_start = def_path + 13;
    if (*id_start == '\0') {
        char *body = build_error_response("Deficiency id required");
        send_http_json(client_fd, 400, "Bad Request", body);
        free(body);
        return true;
    }
    char *endptr = NULL;
    long deficiency_id = strtol(id_start, &endptr, 10);
    if (deficiency_id <= 0 || (endptr && *endptr != '\0')) {
        char *body = build_error_response("Invalid deficiency id");
        send_http_json(client_fd, 400, "Bad Request", body);
        free(body);
        return true;
    }

    bool desired_resolved = false;
    if (!extract_resolved_flag(body_json, &desired_resolved)) {
        char *body = build_error_response("Missing resolved flag");
        send_http_json(client_fd, 400, "Bad Request", body);
        free(body);
        return true;
    }

    bool current_resolved = false;
    char *precheck_error = NULL;
    if (!db_fetch_deficiency_status(conn, target_uuid, deficiency_id, &current_resolved, &precheck_error)) {
        char *body = build_error_response(precheck_error ? precheck_error : "Deficiency lookup failed");
        send_http_json(client_fd, precheck_error && strcmp(precheck_error, "Deficiency not found") == 0 ? 404 : 500,
                       precheck_error && strcmp(precheck_error, "Deficiency not found") == 0 ? "Not Found" : "Internal Server Error",
                       body);
        free(body);
        free(precheck_error);
        return true;
    }
    free(precheck_error);

    if (current_resolved == desired_resolved) {
        char *body = build_error_response("No change required");
        send_http_json(client_fd, 200, "OK", body);
        free(body);
        return true;
    }

    char *resolved_at = NULL;
    char *update_error = NULL;
    if (!db_update_deficiency_status(conn, target_uuid, deficiency_id, desired_resolved, &resolved_at, &update_error)) {
        char *body = build_error_response(update_error ? update_error : "Update failed");
        send_http_json(client_fd, update_error && strcmp(update_error, "Deficiency not found") == 0 ? 404 : 500,
                       update_error && strcmp(update_error, "Deficiency not found") == 0 ? "Not Found" : "Internal Server Error",
                       body);
        free(body);
        free(update_error);
        free(resolved_at);
        return true;
    }
    free(update_error);

    Buffer buf;
    if (!buffer_init(&buf)) {
        free(resolved_at);
        char *body = build_error_response("Out of memory");
        send_http_json(client_fd, 500, "Internal Server Error", body);
        free(body);
        return true;
    }
    buffer_append_cstr(&buf, "{\"status\":\"ok\",\"resolved\":");
    buffer_append_cstr(&buf, desired_resolved ? "true" : "false");
    if (resolved_at) {
        buffer_append_cstr(&buf, ",\"resolved_at\":\"");
        buffer_append_cstr(&buf, resolved_at);
        buffer_append_cstr(&buf, "\"");
    }
    buffer_append_cstr(&buf, "}");
    free(resolved_at);

    char *response = buf.data;
    buf.data = NULL;
    buffer_free(&buf);
    send_http_json(client_fd, 200, "OK", response);
    free(response);
    return true;
}
