#include "db_helpers.h"

#include "buffer.h"
#include "log.h"

#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

char *db_fetch_audit_list(PGconn *conn, char **error_out) {
    const char *sql =
        "SELECT COALESCE(json_agg(row_to_json(t)), '[]'::json)::text "
        "FROM ("
        "  SELECT "
        "    a.audit_uuid,"
        "    a.building_address,"
        "    a.building_owner,"
        "    a.device_type,"
        "    a.bank_name,"
        "    a.city_id,"
        "    a.submitted_on,"
        "    a.updated_at,"
        "    COALESCE((SELECT COUNT(*) FROM audit_deficiencies d WHERE d.audit_uuid = a.audit_uuid AND d.resolved_at IS NULL), 0) AS deficiency_count "
        "  FROM audits a "
        "  ORDER BY a.submitted_on DESC NULLS LAST "
        "  LIMIT 100"
        ") t;";
    PGresult *res = PQexec(conn, sql);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (error_out) {
            const char *msg = PQresultErrorMessage(res);
            *error_out = strdup(msg ? msg : "Database query failed");
        }
        PQclear(res);
        return NULL;
    }
    char *json = NULL;
    if (PQntuples(res) > 0 && !PQgetisnull(res, 0, 0)) {
        const char *value = PQgetvalue(res, 0, 0);
        json = strdup(value ? value : "[]");
    } else {
        json = strdup("[]");
    }
    PQclear(res);
    return json;
}

char *db_fetch_audit_detail(PGconn *conn, const char *uuid, char **error_out) {
    const char *paramValues[1] = { uuid };
    const char *sql =
        "SELECT json_build_object("
        "  'audit', row_to_json(a),"
        "  'deficiencies', COALESCE((SELECT json_agg(row_to_json(d)) FROM audit_deficiencies d WHERE d.audit_uuid = a.audit_uuid), '[]'::json),"
        "  'photos', COALESCE((SELECT json_agg(json_build_object("
        "     'photo_filename', p.photo_filename,"
        "     'content_type', p.content_type,"
        "     'photo_bytes', encode(p.photo_bytes, 'base64')"
        "  )) FROM audit_photos p WHERE p.audit_uuid = a.audit_uuid), '[]'::json)"
        ")::text "
        "FROM audits a "
        "WHERE audit_uuid = $1::uuid;";
    PGresult *res = PQexecParams(conn, sql, 1, NULL, paramValues, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (error_out) {
            const char *msg = PQresultErrorMessage(res);
            *error_out = strdup(msg ? msg : "Database query failed");
        }
        PQclear(res);
        return NULL;
    }
    if (PQntuples(res) == 0 || PQgetisnull(res, 0, 0)) {
        PQclear(res);
        return NULL;
    }
    const char *value = PQgetvalue(res, 0, 0);
    char *json = strdup(value ? value : "{}");
    PQclear(res);
    return json;
}

char *db_fetch_location_list(PGconn *conn, char **error_out) {
    const char *sql =
        "SELECT "
        "  COALESCE(l.location_id, '') AS location_code,"
        "  l.id AS location_row_id,"
        "  a.building_address,"
        "  COALESCE(l.site_name, a.building_address) AS site_name,"
        "  COALESCE(l.street, a.building_address) AS street,"
        "  COALESCE(l.city, a.building_city) AS city,"
        "  COALESCE(l.state, a.building_state) AS state,"
        "  COALESCE(l.zip_code, a.building_postal_code) AS zip,"
        "  MAX(a.building_owner) AS building_owner,"
        "  MAX(a.elevator_contractor) AS elevator_contractor,"
        "  MAX(a.city_id) AS city_id,"
        "  COUNT(*) AS audit_count,"
        "  COUNT(DISTINCT a.building_id) AS device_count,"
        "  MAX(a.submitted_on) AS last_audit,"
        "  MIN(a.submitted_on) AS first_audit,"
        "  COALESCE(SUM(d.open_def_count), 0) AS open_deficiencies"
        " FROM audits a"
        " LEFT JOIN locations l ON a.location_id = l.id"
        " LEFT JOIN ("
        "   SELECT audit_uuid, COUNT(*) FILTER (WHERE resolved_at IS NULL) AS open_def_count"
        "   FROM audit_deficiencies"
        "   GROUP BY audit_uuid"
        " ) d ON d.audit_uuid = a.audit_uuid"
        " WHERE a.building_address IS NOT NULL AND a.building_address <> ''"
        " GROUP BY a.building_address, l.id, l.location_id, l.site_name, l.street, l.city, l.state, l.zip_code"
        " ORDER BY MAX(a.submitted_on) DESC NULLS LAST, a.building_address";

    PGresult *res = PQexec(conn, sql);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (error_out && !*error_out) {
            const char *msg = PQresultErrorMessage(res);
            *error_out = strdup(msg ? msg : "Failed to query locations");
        }
        PQclear(res);
        return NULL;
    }

    Buffer buf;
    if (!buffer_init(&buf)) {
        if (error_out && !*error_out) {
            *error_out = strdup("Out of memory building location response");
        }
        PQclear(res);
        return NULL;
    }

    if (!buffer_append_char(&buf, '[')) {
        buffer_free(&buf);
        PQclear(res);
        if (error_out && !*error_out) {
            *error_out = strdup("Out of memory building location response");
        }
        return NULL;
    }

    int rows = PQntuples(res);
    bool first = true;
    for (int row = 0; row < rows; ++row) {
        const char *location_code = PQgetisnull(res, row, 0) ? NULL : PQgetvalue(res, row, 0);
        const char *row_id_str = PQgetisnull(res, row, 1) ? NULL : PQgetvalue(res, row, 1);
        const char *address = PQgetisnull(res, row, 2) ? NULL : PQgetvalue(res, row, 2);
        if (!address || address[0] == '\0') {
            continue;
        }
        const char *site_name = PQgetisnull(res, row, 3) ? NULL : PQgetvalue(res, row, 3);
        const char *street = PQgetisnull(res, row, 4) ? NULL : PQgetvalue(res, row, 4);
        const char *city = PQgetisnull(res, row, 5) ? NULL : PQgetvalue(res, row, 5);
        const char *state = PQgetisnull(res, row, 6) ? NULL : PQgetvalue(res, row, 6);
        const char *zip = PQgetisnull(res, row, 7) ? NULL : PQgetvalue(res, row, 7);
        const char *owner = PQgetisnull(res, row, 8) ? NULL : PQgetvalue(res, row, 8);
        const char *contractor = PQgetisnull(res, row, 9) ? NULL : PQgetvalue(res, row, 9);
        const char *city_id = PQgetisnull(res, row, 10) ? NULL : PQgetvalue(res, row, 10);
        const char *audit_count_str = PQgetisnull(res, row, 11) ? "0" : PQgetvalue(res, row, 11);
        const char *device_count_str = PQgetisnull(res, row, 12) ? "0" : PQgetvalue(res, row, 12);
        const char *last_audit = PQgetisnull(res, row, 13) ? NULL : PQgetvalue(res, row, 13);
        const char *first_audit = PQgetisnull(res, row, 14) ? NULL : PQgetvalue(res, row, 14);
        const char *open_def_str = PQgetisnull(res, row, 15) ? "0" : PQgetvalue(res, row, 15);

        if (!first) {
            if (!buffer_append_char(&buf, ',')) {
                buffer_free(&buf);
                PQclear(res);
                if (error_out && !*error_out) {
                    *error_out = strdup("Out of memory building location response");
                }
                return NULL;
            }
        }
        first = false;

        if (!buffer_append_char(&buf, '{')) goto oom;
        if (!buffer_append_cstr(&buf, "\"location_code\":")) goto oom;
        if (location_code && location_code[0]) {
            if (!buffer_append_json_string(&buf, location_code)) goto oom;
        } else {
            if (!buffer_append_cstr(&buf, "null")) goto oom;
        }
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"location_row_id\":")) goto oom;
        if (row_id_str && row_id_str[0]) {
            if (!buffer_append_cstr(&buf, row_id_str)) goto oom;
        } else {
            if (!buffer_append_cstr(&buf, "null")) goto oom;
        }
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"address\":")) goto oom;
        if (!buffer_append_json_string(&buf, address)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"site_name\":")) goto oom;
        if (!buffer_append_json_string(&buf, site_name)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"street\":")) goto oom;
        if (!buffer_append_json_string(&buf, street)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"city\":")) goto oom;
        if (!buffer_append_json_string(&buf, city)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"state\":")) goto oom;
        if (!buffer_append_json_string(&buf, state)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"zip\":")) goto oom;
        if (!buffer_append_json_string(&buf, zip)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"building_owner\":")) goto oom;
        if (!buffer_append_json_string(&buf, owner)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"elevator_contractor\":")) goto oom;
        if (!buffer_append_json_string(&buf, contractor)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"city_id\":")) goto oom;
        if (!buffer_append_json_string(&buf, city_id)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"audit_count\":")) goto oom;
        if (!buffer_append_cstr(&buf, audit_count_str)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"device_count\":")) goto oom;
        if (!buffer_append_cstr(&buf, device_count_str)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"open_deficiencies\":")) goto oom;
        if (!buffer_append_cstr(&buf, open_def_str)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"last_audit\":")) goto oom;
        if (!buffer_append_json_string(&buf, last_audit)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"first_audit\":")) goto oom;
        if (!buffer_append_json_string(&buf, first_audit)) goto oom;
        if (!buffer_append_char(&buf, '}')) goto oom;
        continue;

oom:
        buffer_free(&buf);
        PQclear(res);
        if (error_out && !*error_out) {
            *error_out = strdup("Out of memory building location response");
        }
        return NULL;
    }

    if (!buffer_append_char(&buf, ']')) {
        buffer_free(&buf);
        PQclear(res);
        if (error_out && !*error_out) {
            *error_out = strdup("Out of memory building location response");
        }
        return NULL;
    }

    PQclear(res);
    char *json = buf.data;
    buf.data = NULL;
    buffer_free(&buf);
    return json;
}

bool audit_exists(PGconn *conn, const char *uuid) {
    if (!uuid || !*uuid) {
        return false;
    }
    const char *sql = "SELECT 1 FROM audits WHERE audit_uuid = $1::uuid LIMIT 1";
    const char *params[1] = { uuid };
    PGresult *res = PQexecParams(conn, sql, 1, NULL, params, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        log_error("Failed to check for existing audit %s: %s", uuid, PQresultErrorMessage(res));
        PQclear(res);
        return false;
    }
    bool exists = PQntuples(res) > 0;
    PQclear(res);
    return exists;
}

bool db_update_deficiency_status(PGconn *conn, const char *uuid, long deficiency_id, bool resolved, char **resolved_at_out, char **error_out) {
    const char *sql =
        "UPDATE audit_deficiencies "
        "SET resolved_at = CASE WHEN $3::boolean THEN COALESCE(resolved_at, NOW()) ELSE NULL END "
        "WHERE audit_uuid = $1::uuid AND id = $2 "
        "RETURNING resolved_at";
    char id_buf[32];
    snprintf(id_buf, sizeof(id_buf), "%ld", deficiency_id);
    const char *params[3] = { uuid, id_buf, resolved ? "true" : "false" };
    PGresult *res = PQexecParams(conn, sql, 3, NULL, params, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (error_out) {
            const char *msg = PQresultErrorMessage(res);
            *error_out = strdup(msg ? msg : "Failed updating deficiency");
        }
        PQclear(res);
        return false;
    }
    if (PQntuples(res) == 0) {
        PQclear(res);
        if (error_out) {
            *error_out = strdup("Deficiency not found");
        }
        return false;
    }
    if (resolved_at_out) {
        if (PQgetisnull(res, 0, 0)) {
            *resolved_at_out = NULL;
        } else {
            const char *value = PQgetvalue(res, 0, 0);
            *resolved_at_out = value ? strdup(value) : NULL;
        }
    }
    PQclear(res);
    return true;
}

bool db_fetch_deficiency_status(PGconn *conn, const char *uuid, long deficiency_id, bool *resolved_out, char **error_out) {
    if (!conn || !uuid || !resolved_out) {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid parameters while reading deficiency");
        }
        return false;
    }

    const char *sql =
        "SELECT resolved_at IS NOT NULL "
        "FROM audit_deficiencies "
        "WHERE audit_uuid = $1::uuid AND id = $2";
    char id_buf[32];
    snprintf(id_buf, sizeof(id_buf), "%ld", deficiency_id);
    const char *params[2] = { uuid, id_buf };
    PGresult *res = PQexecParams(conn, sql, 2, NULL, params, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (error_out && !*error_out) {
            const char *msg = PQresultErrorMessage(res);
            *error_out = strdup(msg ? msg : "Failed to read deficiency status");
        }
        PQclear(res);
        return false;
    }
    if (PQntuples(res) == 0) {
        if (error_out && !*error_out) {
            *error_out = strdup("Deficiency not found");
        }
        PQclear(res);
        return false;
    }
    if (!PQgetisnull(res, 0, 0)) {
        const char *val = PQgetvalue(res, 0, 0);
        *resolved_out = (strcmp(val, "t") == 0 || strcmp(val, "1") == 0);
    } else {
        *resolved_out = false;
    }
    PQclear(res);
    return true;
}
