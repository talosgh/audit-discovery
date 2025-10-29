#ifndef ROUTES_H
#define ROUTES_H

#include <stdbool.h>
#include <libpq-fe.h>

typedef struct {
    char *path;
    char *filename;
    char *mime;
    char *work_dir;
} ReportDownloadArtifact;

typedef struct {
    char *address;
    char *location_id;
    char *visit_ids;
    char *audit_ids;
} LocationDetailRequest;

typedef struct {
    char *(*build_location_detail)(PGconn *conn, const LocationDetailRequest *request, int *status_out, char **error_out);
    char *(*build_report_json)(PGconn *conn, const LocationDetailRequest *request, int *status_out, char **error_out);
    int (*prepare_report_download)(PGconn *conn, const char *job_id, ReportDownloadArtifact *artifact, char **error_out);
    void (*cleanup_report_download)(ReportDownloadArtifact *artifact);
} RouteHelpers;

void routes_register_helpers(const RouteHelpers *helpers);
void routes_set_prefix(const char *prefix);
void routes_handle_get(int client_fd, PGconn *conn, const char *path, const char *query_string);
bool routes_handle_patch(int client_fd, PGconn *conn, const char *api_path, const char *body_json);

#endif /* ROUTES_H */
