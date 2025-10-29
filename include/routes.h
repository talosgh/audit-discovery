#ifndef ROUTES_H
#define ROUTES_H

#include <stdbool.h>
#include <libpq-fe.h>

typedef struct {
    char *(*build_location_detail)(PGconn *conn, const char *address, int *status_out, char **error_out);
    char *(*build_report_json)(PGconn *conn, const char *address, int *status_out, char **error_out);
} RouteHelpers;

void routes_register_helpers(const RouteHelpers *helpers);
void routes_handle_get(int client_fd, PGconn *conn, const char *path, const char *query_string);
bool routes_handle_patch(int client_fd, PGconn *conn, const char *api_path, const char *body_json);

#endif /* ROUTES_H */
