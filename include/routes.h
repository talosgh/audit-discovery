#ifndef ROUTES_H
#define ROUTES_H

#include <stdbool.h>
#include <libpq-fe.h>

void routes_handle_get(int client_fd, PGconn *conn, const char *path, const char *query_string);
bool routes_handle_patch(int client_fd, PGconn *conn, const char *api_path, const char *body_json);

#endif /* ROUTES_H */
