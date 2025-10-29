#ifndef DB_HELPERS_H
#define DB_HELPERS_H

#include <stdbool.h>
#include <libpq-fe.h>

bool audit_exists(PGconn *conn, const char *uuid);
bool db_update_deficiency_status(PGconn *conn, const char *uuid, long deficiency_id, bool resolved, char **resolved_at_out, char **error_out);
bool db_fetch_deficiency_status(PGconn *conn, const char *uuid, long deficiency_id, bool *resolved_out, char **error_out);
char *db_fetch_audit_list(PGconn *conn, char **error_out);
char *db_fetch_audit_detail(PGconn *conn, const char *uuid, char **error_out);
char *db_fetch_location_list(PGconn *conn, char **error_out);

#endif /* DB_HELPERS_H */
