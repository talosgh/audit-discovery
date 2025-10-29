#ifndef HTTP_H
#define HTTP_H

#include <stdbool.h>
#include <stddef.h>

#include "util.h"

char *build_success_response(const StringArray *audits);
char *build_error_response(const char *message);
void send_http_response(int client_fd, int status_code, const char *status_text, const char *content_type, const void *body, size_t body_len);
void send_http_json(int client_fd, int status_code, const char *status_text, const char *json_body);
void send_file_download(int client_fd, const char *path, const char *content_type, const char *filename);
void serve_static_file(int client_fd, const char *path);
const char *mime_type_for(const char *path);
bool path_is_safe(const char *path);
char *http_extract_query_param(const char *query_string, const char *key);

#endif /* HTTP_H */
