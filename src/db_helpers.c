#include "db_helpers.h"

#include "buffer.h"
#include "log.h"

#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef TEXTOID
#define TEXTOID 25
#endif
#ifndef INT8OID
#define INT8OID 20
#endif
#ifndef INT4OID
#define INT4OID 23
#endif

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

char *db_fetch_location_list(PGconn *conn, int page, int page_size, const char *search, char **error_out) {
    if (!conn) {
        if (error_out && !*error_out) {
            *error_out = strdup("Database connection unavailable");
        }
        return NULL;
    }

    if (page < 1) page = 1;
    if (page_size < 1) page_size = 25;
    if (page_size > 200) page_size = 200;

    char page_buf[32];
    char limit_buf[32];
    snprintf(page_buf, sizeof(page_buf), "%d", page);
    snprintf(limit_buf, sizeof(limit_buf), "%d", page_size);

    char *pattern = NULL;
    if (search && search[0]) {
        size_t len = strlen(search);
        pattern = malloc(len + 3);
        if (!pattern) {
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory preparing search pattern");
            }
            return NULL;
        }
        pattern[0] = '%';
        memcpy(pattern + 1, search, len);
        pattern[len + 1] = '%';
        pattern[len + 2] = '\0';
    }

    static const char *COUNT_SQL =
        "SELECT COUNT(*) "
        "FROM locations l "
        "WHERE ($1 IS NULL OR "
        "       l.location_id ILIKE $1 OR "
        "       l.site_name ILIKE $1 OR "
        "       l.street ILIKE $1 OR "
        "       l.city ILIKE $1 OR "
        "       l.state ILIKE $1 OR "
        "       l.zip_code ILIKE $1 OR "
        "       l.owner_name ILIKE $1 OR "
        "       l.vendor_name ILIKE $1)";

    const char *count_params[1] = { pattern };
    const Oid count_types[1] = { TEXTOID };
    PGresult *count_res = PQexecParams(conn, COUNT_SQL, 1, count_types, count_params, NULL, NULL, 0);
    long long total = 0;
    if (!count_res || PQresultStatus(count_res) != PGRES_TUPLES_OK) {
        if (error_out && !*error_out) {
            const char *msg = count_res ? PQresultErrorMessage(count_res) : PQerrorMessage(conn);
            *error_out = strdup(msg ? msg : "Failed to count locations");
        }
        if (count_res) PQclear(count_res);
        free(pattern);
        return NULL;
    }
    if (PQntuples(count_res) > 0 && !PQgetisnull(count_res, 0, 0)) {
        total = atoll(PQgetvalue(count_res, 0, 0));
    }
    PQclear(count_res);

    static const char *ITEM_SQL =
        "WITH stats AS ("
        "    SELECT a.location_id,"
        "           SUM(CASE WHEN d.resolved_at IS NULL THEN 1 ELSE 0 END) AS open_deficiencies,"
        "           COUNT(*) > 0 AS has_audits "
        "    FROM audits a "
        "    LEFT JOIN audit_deficiencies d ON d.audit_uuid = a.audit_uuid "
        "    WHERE a.location_id IS NOT NULL "
        "    GROUP BY a.location_id"
        "), "
        "service AS ("
        "    SELECT sd_location_id AS location_code "
        "    FROM esa_in_progress "
        "    WHERE sd_location_id IS NOT NULL "
        "    GROUP BY sd_location_id"
        "), "
        "finance AS ("
        "    SELECT location_id "
        "    FROM financial_data "
        "    WHERE location_id IS NOT NULL "
        "    GROUP BY location_id"
        ") "
        "SELECT "
        "    l.location_id AS location_code, "
        "    l.id AS location_row_id, "
        "    l.site_name, "
        "    l.street, "
        "    l.city, "
        "    l.state, "
        "    CONCAT_WS(', ', NULLIF(l.street, ''), NULLIF(l.city, ''), NULLIF(l.state, '')) AS formatted_address, "
        "    l.zip_code, "
        "    l.owner_name, "
        "    l.vendor_name, "
        "    COALESCE(l.units, 0) AS device_count, "
        "    COALESCE(stats.open_deficiencies, 0) AS open_deficiencies, "
        "    COALESCE(stats.has_audits, false) AS has_audits, "
        "    (service.location_code IS NOT NULL) AS has_service_records, "
        "    (finance.location_id IS NOT NULL) AS has_financial_records "
        "FROM locations l "
        "LEFT JOIN stats ON stats.location_id = l.id "
        "LEFT JOIN service ON service.location_code = l.location_id "
        "LEFT JOIN finance ON ("
        "       finance.location_id = l.id "
        "    OR (l.location_id ~ '^[0-9]+$' AND finance.location_id = l.location_id::int)"
        "    ) "
        "WHERE ($3 IS NULL OR "
        "       l.location_id ILIKE $3 OR "
        "       l.site_name ILIKE $3 OR "
        "       l.street ILIKE $3 OR "
        "       l.city ILIKE $3 OR "
        "       l.state ILIKE $3 OR "
        "       l.zip_code ILIKE $3 OR "
        "       l.owner_name ILIKE $3 OR "
        "       l.vendor_name ILIKE $3) "
        "ORDER BY lower(COALESCE(NULLIF(l.site_name, ''), NULLIF(l.street, ''), l.city, l.state, l.location_id)), l.id "
        "OFFSET GREATEST($1::int - 1, 0) * $2::int "
        "LIMIT $2::int";

    const char *item_params[3] = { page_buf, limit_buf, pattern };
    const Oid item_types[3] = { INT4OID, INT4OID, TEXTOID };
    PGresult *res = PQexecParams(conn, ITEM_SQL, 3, item_types, item_params, NULL, NULL, 0);
    free(pattern);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (error_out && !*error_out) {
            const char *msg = res ? PQresultErrorMessage(res) : PQerrorMessage(conn);
            *error_out = strdup(msg ? msg : "Failed to query locations");
        }
        if (res) PQclear(res);
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

    if (!buffer_append_char(&buf, '{')) goto oom;
    if (!buffer_append_cstr(&buf, "\"page\":")) goto oom;
    if (!buffer_appendf(&buf, "%d", page)) goto oom;
    if (!buffer_append_cstr(&buf, ",\"page_size\":")) goto oom;
    if (!buffer_appendf(&buf, "%d", page_size)) goto oom;
    if (!buffer_append_cstr(&buf, ",\"total\":")) goto oom;
    if (!buffer_appendf(&buf, "%lld", total)) goto oom;
    if (!buffer_append_cstr(&buf, ",\"items\":[")) goto oom;

    int rows = PQntuples(res);
    bool first = true;
    for (int row = 0; row < rows; ++row) {
        const char *location_code = PQgetisnull(res, row, 0) ? NULL : PQgetvalue(res, row, 0);
        const char *row_id = PQgetisnull(res, row, 1) ? NULL : PQgetvalue(res, row, 1);
        const char *site_name = PQgetisnull(res, row, 2) ? NULL : PQgetvalue(res, row, 2);
        const char *street = PQgetisnull(res, row, 3) ? NULL : PQgetvalue(res, row, 3);
        const char *city = PQgetisnull(res, row, 4) ? NULL : PQgetvalue(res, row, 4);
        const char *state = PQgetisnull(res, row, 5) ? NULL : PQgetvalue(res, row, 5);
        const char *formatted = PQgetisnull(res, row, 6) ? NULL : PQgetvalue(res, row, 6);
        const char *zip = PQgetisnull(res, row, 7) ? NULL : PQgetvalue(res, row, 7);
        const char *owner = PQgetisnull(res, row, 8) ? NULL : PQgetvalue(res, row, 8);
        const char *vendor = PQgetisnull(res, row, 9) ? NULL : PQgetvalue(res, row, 9);
        const char *device_count = PQgetisnull(res, row, 10) ? "0" : PQgetvalue(res, row, 10);
        const char *open_def = PQgetisnull(res, row, 11) ? "0" : PQgetvalue(res, row, 11);
        const char *has_audits = PQgetisnull(res, row, 12) ? "f" : PQgetvalue(res, row, 12);
        const char *has_service = PQgetisnull(res, row, 13) ? "f" : PQgetvalue(res, row, 13);
        const char *has_financial = PQgetisnull(res, row, 14) ? "f" : PQgetvalue(res, row, 14);

        if (!first) {
            if (!buffer_append_char(&buf, ',')) goto oom;
        }
        first = false;

        if (!buffer_append_char(&buf, '{')) goto oom;
        if (!buffer_append_cstr(&buf, "\"location_code\":")) goto oom;
        if (!buffer_append_json_string(&buf, location_code)) goto oom;
        if (!buffer_append_cstr(&buf, ",\"location_row_id\":")) goto oom;
        if (row_id && row_id[0]) {
            if (!buffer_append_cstr(&buf, row_id)) goto oom;
        } else {
            if (!buffer_append_cstr(&buf, "null")) goto oom;
        }
        const char *display_address = (formatted && formatted[0])
            ? formatted
            : (street && street[0] ? street : (site_name && site_name[0] ? site_name : location_code));
        if (!buffer_append_cstr(&buf, ",\"address\":")) goto oom;
        if (!buffer_append_json_string(&buf, display_address)) goto oom;
        if (!buffer_append_cstr(&buf, ",\"formatted_address\":")) goto oom;
        if (!buffer_append_json_string(&buf, display_address)) goto oom;
        if (!buffer_append_cstr(&buf, ",\"site_name\":")) goto oom;
        if (!buffer_append_json_string(&buf, site_name)) goto oom;
        if (!buffer_append_cstr(&buf, ",\"street\":")) goto oom;
        if (!buffer_append_json_string(&buf, street)) goto oom;
        if (!buffer_append_cstr(&buf, ",\"city\":")) goto oom;
        if (!buffer_append_json_string(&buf, city)) goto oom;
        if (!buffer_append_cstr(&buf, ",\"state\":")) goto oom;
        if (!buffer_append_json_string(&buf, state)) goto oom;
        if (!buffer_append_cstr(&buf, ",\"zip\":")) goto oom;
        if (!buffer_append_json_string(&buf, zip)) goto oom;
        if (!buffer_append_cstr(&buf, ",\"building_owner\":")) goto oom;
        if (!buffer_append_json_string(&buf, owner)) goto oom;
        if (!buffer_append_cstr(&buf, ",\"vendor_name\":")) goto oom;
        if (!buffer_append_json_string(&buf, vendor)) goto oom;
        if (!buffer_append_cstr(&buf, ",\"device_count\":")) goto oom;
        if (!buffer_append_cstr(&buf, device_count)) goto oom;
        if (!buffer_append_cstr(&buf, ",\"open_deficiencies\":")) goto oom;
        if (!buffer_append_cstr(&buf, open_def)) goto oom;
        if (!buffer_append_cstr(&buf, ",\"has_audits\":")) goto oom;
        if (!buffer_append_cstr(&buf, (has_audits && (has_audits[0] == 't' || has_audits[0] == '1')) ? "true" : "false")) goto oom;
        if (!buffer_append_cstr(&buf, ",\"has_service_records\":")) goto oom;
        if (!buffer_append_cstr(&buf, (has_service && (has_service[0] == 't' || has_service[0] == '1')) ? "true" : "false")) goto oom;
        if (!buffer_append_cstr(&buf, ",\"has_financial_records\":")) goto oom;
        if (!buffer_append_cstr(&buf, (has_financial && (has_financial[0] == 't' || has_financial[0] == '1')) ? "true" : "false")) goto oom;
        if (!buffer_append_char(&buf, '}')) goto oom;
    }

    if (!buffer_append_char(&buf, ']')) goto oom;
    if (!buffer_append_char(&buf, '}')) goto oom;

    PQclear(res);
    char *json = buf.data;
    buf.data = NULL;
    buffer_free(&buf);
    return json;

oom:
    buffer_free(&buf);
    PQclear(res);
    if (error_out && !*error_out) {
        *error_out = strdup("Out of memory building location response");
    }
    return NULL;
}

char *db_fetch_metrics_summary(PGconn *conn, char **error_out) {
    if (error_out) {
        *error_out = NULL;
    }
    if (!conn) {
        if (error_out && !*error_out) {
            *error_out = strdup("Database connection unavailable");
        }
        return NULL;
    }

    const char *sql =
        "SELECT "
        "    COALESCE(SUM(COALESCE(delta, 0)), 0)::numeric AS total_savings, "
        "    COALESCE(SUM(COALESCE(proposed_cost, 0)), 0)::numeric AS total_proposed, "
        "    COALESCE(SUM(COALESCE(new_cost, 0)), 0)::numeric AS total_spend "
        "FROM financial_data";

    PGresult *res = PQexec(conn, sql);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (error_out && !*error_out) {
            const char *msg = res ? PQresultErrorMessage(res) : PQerrorMessage(conn);
            *error_out = strdup(msg ? msg : "Failed to load metrics summary");
        }
        if (res) {
            PQclear(res);
        }
        return NULL;
    }

    double total_savings = 0.0;
    double total_proposed = 0.0;
    double total_spend = 0.0;
    if (PQntuples(res) > 0) {
        if (!PQgetisnull(res, 0, 0)) {
            total_savings = strtod(PQgetvalue(res, 0, 0), NULL);
        }
        if (!PQgetisnull(res, 0, 1)) {
            total_proposed = strtod(PQgetvalue(res, 0, 1), NULL);
        }
        if (!PQgetisnull(res, 0, 2)) {
            total_spend = strtod(PQgetvalue(res, 0, 2), NULL);
        }
    }
    PQclear(res);

    Buffer buf;
    if (!buffer_init(&buf)) {
        if (error_out && !*error_out) {
            *error_out = strdup("Out of memory assembling metrics summary");
        }
        return NULL;
    }

    if (!buffer_append_char(&buf, '{')) goto metrics_oom;
    if (!buffer_append_cstr(&buf, "\"total_savings\":")) goto metrics_oom;
    if (!buffer_appendf(&buf, "%.2f", total_savings)) goto metrics_oom;
    if (!buffer_append_char(&buf, ',')) goto metrics_oom;
    if (!buffer_append_cstr(&buf, "\"total_proposed\":")) goto metrics_oom;
    if (!buffer_appendf(&buf, "%.2f", total_proposed)) goto metrics_oom;
    if (!buffer_append_char(&buf, ',')) goto metrics_oom;
    if (!buffer_append_cstr(&buf, "\"total_spend\":")) goto metrics_oom;
    if (!buffer_appendf(&buf, "%.2f", total_spend)) goto metrics_oom;
    if (!buffer_append_char(&buf, '}')) goto metrics_oom;

    return buf.data;

metrics_oom:
    buffer_free(&buf);
    if (error_out && !*error_out) {
        *error_out = strdup("Out of memory assembling metrics summary");
    }
    return NULL;
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
