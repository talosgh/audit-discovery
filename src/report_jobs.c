#include "report_jobs.h"

#include "buffer.h"
#include "log.h"

#include <stdbool.h>
#include <stdio.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

static const char *report_job_type_to_string(ReportJobType type) {
    switch (type) {
        case REPORT_JOB_TYPE_LOCATION_OVERVIEW:
            return "overview";
        case REPORT_JOB_TYPE_AUDIT:
        default:
            return "audit";
    }
}

static ReportJobType report_job_type_from_string(const char *value) {
    if (!value || value[0] == '\0') {
        return REPORT_JOB_TYPE_AUDIT;
    }
    if (strcasecmp(value, "overview") == 0 || strcasecmp(value, "location_overview") == 0) {
        return REPORT_JOB_TYPE_LOCATION_OVERVIEW;
    }
    if (strcasecmp(value, "audit") == 0 || strcasecmp(value, "full") == 0) {
        return REPORT_JOB_TYPE_AUDIT;
    }
    return REPORT_JOB_TYPE_AUDIT;
}

void report_job_init(ReportJob *job) {
    if (!job) {
        return;
    }
    job->job_id[0] = '\0';
    job->address = NULL;
    job->notes = NULL;
    job->recommendations = NULL;
    job->cover_building_owner = NULL;
    job->cover_street = NULL;
    job->cover_city = NULL;
    job->cover_state = NULL;
    job->cover_zip = NULL;
    job->cover_contact_name = NULL;
    job->cover_contact_email = NULL;
    job->type = REPORT_JOB_TYPE_AUDIT;
    job->deficiency_only = false;
    job->include_all = true;
    job->has_location_id = false;
    job->location_id = 0;
    job->range_start = NULL;
    job->range_end = NULL;
    job->range_preset = NULL;
    string_array_init(&job->audit_ids);
}

void report_job_clear(ReportJob *job) {
    if (!job) {
        return;
    }
    free(job->address);
    free(job->notes);
    free(job->recommendations);
    free(job->cover_building_owner);
    free(job->cover_street);
    free(job->cover_city);
    free(job->cover_state);
    free(job->cover_zip);
    free(job->cover_contact_name);
    free(job->cover_contact_email);
    free(job->range_start);
    free(job->range_end);
    free(job->range_preset);
    string_array_clear(&job->audit_ids);
    job->address = NULL;
    job->notes = NULL;
    job->recommendations = NULL;
    job->cover_building_owner = NULL;
    job->cover_street = NULL;
    job->cover_city = NULL;
    job->cover_state = NULL;
    job->cover_zip = NULL;
    job->cover_contact_name = NULL;
    job->cover_contact_email = NULL;
    job->job_id[0] = '\0';
    job->type = REPORT_JOB_TYPE_AUDIT;
    job->deficiency_only = false;
    job->include_all = true;
    job->has_location_id = false;
    job->location_id = 0;
    job->range_start = NULL;
    job->range_end = NULL;
    job->range_preset = NULL;
}

static int db_insert_job_audits(PGconn *conn, const char *job_id, const StringArray *audits, char **error_out) {
    if (!audits || audits->count == 0) {
        return 1;
    }
    if (!conn || !job_id || job_id[0] == '\0') {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid parameters inserting report job audits");
        }
        return 0;
    }

    const char *sql = "INSERT INTO report_job_audits (job_id, audit_uuid) VALUES ($1::uuid, $2::uuid)";
    for (size_t i = 0; i < audits->count; ++i) {
        const char *audit_id = audits->values[i];
        if (!audit_id || !is_valid_uuid(audit_id)) {
            if (error_out && !*error_out) {
                *error_out = strdup("Invalid audit identifier in job selection");
            }
            return 0;
        }
        const char *params[2] = { job_id, audit_id };
        PGresult *res = PQexecParams(conn, sql, 2, NULL, params, NULL, NULL, 0);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            if (error_out && !*error_out) {
                const char *msg = PQresultErrorMessage(res);
                *error_out = strdup(msg ? msg : "Failed to insert report job audit selection");
            }
            PQclear(res);
            return 0;
        }
        PQclear(res);
    }
    return 1;
}

int db_insert_report_job(PGconn *conn, const char *job_id, const ReportJob *job, char **error_out) {
    if (!conn || !job_id || !job || !job->address || job->address[0] == '\0') {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid report job parameters");
        }
        return 0;
    }
    bool include_all = job->include_all || job->audit_ids.count == 0;

    const char *sql =
        "INSERT INTO report_jobs (job_id, address, notes, recommendations, "
        "cover_building_owner, cover_street, cover_city, cover_state, cover_zip, cover_contact_name, cover_contact_email, deficiency_only, job_type, range_start, range_end, range_preset, location_id, include_all) "
        "VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::boolean, $13, $14::date, $15::date, $16, $17::int, $18::boolean)";
    char location_buf[32];
    const char *location_param = NULL;
    if (job->has_location_id) {
        snprintf(location_buf, sizeof(location_buf), "%d", job->location_id);
        location_param = location_buf;
    }
    const char *params[18] = {
        job_id,
        job->address,
        job->notes,
        job->recommendations,
        job->cover_building_owner,
        job->cover_street,
        job->cover_city,
        job->cover_state,
        job->cover_zip,
        job->cover_contact_name,
        job->cover_contact_email,
        job->deficiency_only ? "true" : "false",
        report_job_type_to_string(job->type),
        job->range_start,
        job->range_end,
        job->range_preset,
        location_param,
        include_all ? "true" : "false"
    };
    int param_count = 18;
    PGresult *res = PQexecParams(conn, sql, param_count, NULL, params, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        if (error_out && !*error_out) {
            const char *msg = PQresultErrorMessage(res);
            *error_out = strdup(msg ? msg : "Failed to insert report job");
        }
        PQclear(res);
        return 0;
    }
    PQclear(res);

    if (job->audit_ids.count > 0) {
        char *audit_error = NULL;
        if (!db_insert_job_audits(conn, job_id, &job->audit_ids, &audit_error)) {
            const char *del_params[1] = { job_id };
            PGresult *del_res = PQexecParams(conn, "DELETE FROM report_jobs WHERE job_id = $1::uuid", 1, NULL, del_params, NULL, NULL, 0);
            if (del_res) {
                PQclear(del_res);
            }
            if (error_out && !*error_out) {
                *error_out = audit_error ? audit_error : strdup("Failed to insert report job audits");
            } else {
                free(audit_error);
            }
            return 0;
        }
        free(audit_error);
    }

    return 1;
}

int db_claim_next_report_job(PGconn *conn, ReportJob *job, char **error_out) {
    if (!conn || !job) {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid report job request");
        }
        return -1;
    }
    const char *sql =
        "WITH job AS ("
        "    SELECT id, job_id::text AS job_id_text, address, notes, recommendations, "
        "           cover_building_owner, cover_street, cover_city, cover_state, cover_zip, cover_contact_name, cover_contact_email, deficiency_only, job_type, range_start, range_end, range_preset, location_id, include_all "
        "    FROM report_jobs "
        "    WHERE status = 'queued' "
        "    ORDER BY created_at "
        "    LIMIT 1 "
        "    FOR UPDATE SKIP LOCKED"
        ") "
        "UPDATE report_jobs r "
        "SET status = 'processing', started_at = COALESCE(r.started_at, NOW()), updated_at = NOW() "
        "FROM job "
        "WHERE r.id = job.id "
        "RETURNING job.job_id_text, job.address, job.notes, job.recommendations, "
        "          job.cover_building_owner, job.cover_street, job.cover_city, job.cover_state, job.cover_zip, job.cover_contact_name, job.cover_contact_email, job.deficiency_only, job.job_type, job.range_start, job.range_end, job.range_preset, job.location_id, job.include_all";
    PGresult *res = PQexec(conn, sql);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (error_out && !*error_out) {
            const char *msg = PQresultErrorMessage(res);
            *error_out = strdup(msg ? msg : "Failed to claim report job");
        }
        PQclear(res);
        return -1;
    }
    int rows = PQntuples(res);
    if (rows == 0) {
        PQclear(res);
        return 0;
    }
    report_job_clear(job);
    report_job_init(job);
    const char *job_id = PQgetvalue(res, 0, 0);
    if (!job_id || strlen(job_id) >= sizeof(job->job_id)) {
        PQclear(res);
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid job identifier");
        }
        return -1;
    }
    strncpy(job->job_id, job_id, sizeof(job->job_id));
    job->job_id[36] = '\0';

    if (!PQgetisnull(res, 0, 1)) {
        job->address = strdup(PQgetvalue(res, 0, 1));
        if (!job->address) {
            PQclear(res);
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory copying address");
            }
            return -1;
        }
    }
    if (!PQgetisnull(res, 0, 2)) {
        job->notes = strdup(PQgetvalue(res, 0, 2));
        if (!job->notes) {
            PQclear(res);
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory copying notes");
            }
            return -1;
        }
    }
    if (!PQgetisnull(res, 0, 3)) {
        job->recommendations = strdup(PQgetvalue(res, 0, 3));
        if (!job->recommendations) {
            PQclear(res);
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory copying recommendations");
            }
            return -1;
        }
    }
    if (!PQgetisnull(res, 0, 4)) {
        job->cover_building_owner = strdup(PQgetvalue(res, 0, 4));
        if (!job->cover_building_owner) {
            PQclear(res);
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory copying cover owner");
            }
            return -1;
        }
    }
    if (!PQgetisnull(res, 0, 5)) {
        job->cover_street = strdup(PQgetvalue(res, 0, 5));
        if (!job->cover_street) {
            PQclear(res);
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory copying cover street");
            }
            return -1;
        }
    }
    if (!PQgetisnull(res, 0, 6)) {
        job->cover_city = strdup(PQgetvalue(res, 0, 6));
        if (!job->cover_city) {
            PQclear(res);
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory copying cover city");
            }
            return -1;
        }
    }
    if (!PQgetisnull(res, 0, 7)) {
        job->cover_state = strdup(PQgetvalue(res, 0, 7));
        if (!job->cover_state) {
            PQclear(res);
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory copying cover state");
            }
            return -1;
        }
    }
    if (!PQgetisnull(res, 0, 8)) {
        job->cover_zip = strdup(PQgetvalue(res, 0, 8));
        if (!job->cover_zip) {
            PQclear(res);
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory copying cover zip");
            }
            return -1;
        }
    }
    if (!PQgetisnull(res, 0, 9)) {
        job->cover_contact_name = strdup(PQgetvalue(res, 0, 9));
        if (!job->cover_contact_name) {
            PQclear(res);
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory copying cover contact name");
            }
            return -1;
        }
    }
    if (!PQgetisnull(res, 0, 10)) {
        job->cover_contact_email = strdup(PQgetvalue(res, 0, 10));
        if (!job->cover_contact_email) {
            PQclear(res);
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory copying cover contact email");
            }
            return -1;
        }
    }
    job->deficiency_only = !PQgetisnull(res, 0, 11) && (strcmp(PQgetvalue(res, 0, 11), "t") == 0 || strcmp(PQgetvalue(res, 0, 11), "true") == 0 || strcmp(PQgetvalue(res, 0, 11), "1") == 0);
    job->type = report_job_type_from_string(PQgetisnull(res, 0, 12) ? NULL : PQgetvalue(res, 0, 12));
    if (!PQgetisnull(res, 0, 13)) {
        job->range_start = strdup(PQgetvalue(res, 0, 13));
        if (!job->range_start) {
            PQclear(res);
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory copying range_start");
            }
            return -1;
        }
    }
    if (!PQgetisnull(res, 0, 14)) {
        job->range_end = strdup(PQgetvalue(res, 0, 14));
        if (!job->range_end) {
            PQclear(res);
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory copying range_end");
            }
            return -1;
        }
    }
    if (!PQgetisnull(res, 0, 15)) {
        job->range_preset = strdup(PQgetvalue(res, 0, 15));
        if (!job->range_preset) {
            PQclear(res);
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory copying range preset");
            }
            return -1;
        }
    }
    job->has_location_id = false;
    job->location_id = 0;
    if (!PQgetisnull(res, 0, 16)) {
        job->has_location_id = true;
        job->location_id = atoi(PQgetvalue(res, 0, 16));
    }
    job->include_all = PQgetisnull(res, 0, 17) || (strcmp(PQgetvalue(res, 0, 17), "t") == 0 || strcmp(PQgetvalue(res, 0, 17), "true") == 0 || strcmp(PQgetvalue(res, 0, 17), "1") == 0);
    PQclear(res);

    const char *audit_params[1] = { job->job_id };
    PGresult *audit_res = PQexecParams(conn, "SELECT audit_uuid::text FROM report_job_audits WHERE job_id = $1::uuid ORDER BY audit_uuid", 1, NULL, audit_params, NULL, NULL, 0);
    if (!audit_res || PQresultStatus(audit_res) != PGRES_TUPLES_OK) {
        if (error_out && !*error_out) {
            const char *msg = audit_res ? PQresultErrorMessage(audit_res) : NULL;
            *error_out = strdup(msg ? msg : "Failed to load report job audits");
        }
        if (audit_res) {
            PQclear(audit_res);
        }
        return -1;
    }
    string_array_clear(&job->audit_ids);
    int audit_rows = PQntuples(audit_res);
    for (int i = 0; i < audit_rows; ++i) {
        const char *audit_id = PQgetisnull(audit_res, i, 0) ? NULL : PQgetvalue(audit_res, i, 0);
        if (!audit_id) {
            continue;
        }
        if (!string_array_append_copy(&job->audit_ids, audit_id)) {
            PQclear(audit_res);
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory loading job audits");
            }
            return -1;
        }
    }
    PQclear(audit_res);
    if (job->audit_ids.count > 0) {
        job->include_all = false;
    }
    return 1;
}

int db_complete_report_job(PGconn *conn,
                           const char *job_id,
                           const char *status,
                           const char *error_text,
                           const char *artifact_filename,
                           const char *artifact_mime,
                           const unsigned char *artifact_bytes,
                           size_t artifact_size,
                           char **error_out) {
    if (!conn || !job_id || !status) {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid report job completion parameters");
        }
        return 0;
    }

    bool completed = strcmp(status, "completed") == 0;
    const char *resolved_mime = artifact_mime;
    if (completed) {
        if (!artifact_bytes || artifact_size == 0 || !artifact_filename || artifact_filename[0] == '\0') {
            if (error_out && !*error_out) {
                *error_out = strdup("Artifact data missing for completed report");
            }
            return 0;
        }
        if (!resolved_mime || resolved_mime[0] == '\0') {
            resolved_mime = "application/pdf";
        }
        if (artifact_size > (size_t)INT_MAX) {
            if (error_out && !*error_out) {
                *error_out = strdup("Artifact too large to store");
            }
            return 0;
        }
    } else {
        artifact_bytes = NULL;
        artifact_size = 0;
        artifact_filename = NULL;
        resolved_mime = NULL;
    }

    char size_buf[32];
    const char *size_param = NULL;
    if (completed) {
        snprintf(size_buf, sizeof(size_buf), "%zu", artifact_size);
        size_param = size_buf;
    }

    const char *sql =
        "WITH target AS ("
        "        SELECT id, address, deficiency_only, job_type "
        "        FROM report_jobs "
        "        WHERE job_id = $1::uuid"
        "    ), version_calc AS ("
        "        SELECT target.id, "
        "               CASE WHEN $2 = 'completed' THEN "
        "                    COALESCE(("
        "                        SELECT MAX(artifact_version) "
        "                        FROM report_jobs "
        "                        WHERE address = target.address "
        "                          AND deficiency_only = target.deficiency_only "
        "                          AND job_type = target.job_type"
        "                    ), 0) + 1 "
        "               ELSE NULL END AS next_version "
        "        FROM target"
        "    ) "
        "UPDATE report_jobs r "
        "SET status = $2, "
        "    error = $3, "
        "    output_path = NULL, "
        "    artifact_filename = CASE WHEN $2 = 'completed' THEN $4 ELSE NULL END, "
        "    artifact_mime = CASE WHEN $2 = 'completed' THEN $5 ELSE NULL END, "
        "    artifact_bytes = CASE WHEN $2 = 'completed' THEN $6::bytea ELSE NULL END, "
        "    artifact_size = CASE WHEN $2 = 'completed' THEN $7::bigint ELSE NULL END, "
        "    artifact_version = CASE WHEN $2 = 'completed' THEN COALESCE((SELECT next_version FROM version_calc), 1) ELSE artifact_version END, "
        "    completed_at = CASE WHEN $2 IN ('completed','failed') THEN NOW() ELSE completed_at END, "
        "    updated_at = NOW() "
        "FROM target "
        "LEFT JOIN version_calc ON version_calc.id = target.id "
        "WHERE r.id = target.id";

    const char *paramValues[7] = {0};
    int paramLengths[7] = {0};
    int paramFormats[7] = {0};
    Oid paramTypes[7] = {0};

    paramValues[0] = job_id;
    paramValues[1] = status;
    paramValues[2] = error_text;
    paramValues[3] = artifact_filename;
    paramValues[4] = resolved_mime;
    if (completed) {
        paramValues[5] = (const char *)artifact_bytes;
        paramLengths[5] = (int)artifact_size;
        paramFormats[5] = 1;
    } else {
        paramValues[5] = NULL;
        paramLengths[5] = 0;
        paramFormats[5] = 0;
    }
    paramTypes[5] = 17; /* BYTEAOID */
    paramValues[6] = size_param;
    paramTypes[6] = 20; /* INT8OID */

    PGresult *res = PQexecParams(conn, sql, 7, paramTypes, paramValues, paramLengths, paramFormats, 0);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        if (error_out && !*error_out) {
            const char *msg = PQresultErrorMessage(res);
            *error_out = strdup(msg ? msg : "Failed updating report job");
        }
        PQclear(res);
        return 0;
    }
    if (PQcmdTuples(res)[0] == '0') {
        if (error_out && !*error_out) {
            *error_out = strdup("Report job not found");
        }
        PQclear(res);
        return 0;
    }
    PQclear(res);
    return 1;
}

char *db_fetch_report_job_status(PGconn *conn, const char *job_id, const char *path_prefix, char **error_out) {
    if (!conn || !job_id) {
        if (error_out && !*error_out) {
            *error_out = strdup("Job id required");
        }
        return NULL;
    }
    const char *sql =
        "SELECT r.job_id::text, r.status, r.address, "
        "       to_char(r.created_at, 'YYYY-MM-DD" "T" "HH24:MI:SSOF'), "
        "       to_char(r.started_at, 'YYYY-MM-DD" "T" "HH24:MI:SSOF'), "
        "       to_char(r.completed_at, 'YYYY-MM-DD" "T" "HH24:MI:SSOF'), "
        "       r.error, "
        "       r.deficiency_only, "
        "       r.job_type, "
        "       to_char(r.range_start, 'YYYY-MM-DD'), "
        "       to_char(r.range_end, 'YYYY-MM-DD'), "
        "       r.range_preset, "
        "       r.include_all, "
        "       r.location_id, "
        "       r.artifact_size, "
        "       r.artifact_filename, "
        "       r.artifact_version, "
        "       COALESCE(sel.selection_count, 0) "
        "FROM report_jobs r "
        "LEFT JOIN ("
        "    SELECT job_id, COUNT(*) AS selection_count "
        "    FROM report_job_audits "
        "    GROUP BY job_id"
        ") sel ON sel.job_id = r.job_id "
        "WHERE r.job_id = $1::uuid";
    const char *params[1] = { job_id };
    PGresult *res = PQexecParams(conn, sql, 1, NULL, params, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (error_out && !*error_out) {
            const char *msg = PQresultErrorMessage(res);
            *error_out = strdup(msg ? msg : "Failed to fetch report job");
        }
        PQclear(res);
        return NULL;
    }
    if (PQntuples(res) == 0) {
        PQclear(res);
        if (error_out && !*error_out) {
            *error_out = strdup("Report job not found");
        }
        return NULL;
    }

    const char *job_id_val = PQgetvalue(res, 0, 0);
    const char *status_val = PQgetvalue(res, 0, 1);
    const char *address_val = PQgetisnull(res, 0, 2) ? NULL : PQgetvalue(res, 0, 2);
    const char *created_val = PQgetisnull(res, 0, 3) ? NULL : PQgetvalue(res, 0, 3);
    const char *started_val = PQgetisnull(res, 0, 4) ? NULL : PQgetvalue(res, 0, 4);
    const char *completed_val = PQgetisnull(res, 0, 5) ? NULL : PQgetvalue(res, 0, 5);
    const char *error_val = PQgetisnull(res, 0, 6) ? NULL : PQgetvalue(res, 0, 6);
    const char *deficiency_only_val = PQgetisnull(res, 0, 7) ? NULL : PQgetvalue(res, 0, 7);
    const char *job_type_val = PQgetisnull(res, 0, 8) ? NULL : PQgetvalue(res, 0, 8);
    const char *range_start_val = PQgetisnull(res, 0, 9) ? NULL : PQgetvalue(res, 0, 9);
    const char *range_end_val = PQgetisnull(res, 0, 10) ? NULL : PQgetvalue(res, 0, 10);
    const char *range_preset_val = PQgetisnull(res, 0, 11) ? NULL : PQgetvalue(res, 0, 11);
    const char *include_all_val = PQgetisnull(res, 0, 12) ? NULL : PQgetvalue(res, 0, 12);
    const char *location_id_val = PQgetisnull(res, 0, 13) ? NULL : PQgetvalue(res, 0, 13);
    const char *artifact_size_val = PQgetisnull(res, 0, 14) ? NULL : PQgetvalue(res, 0, 14);
    const char *artifact_filename_val = PQgetisnull(res, 0, 15) ? NULL : PQgetvalue(res, 0, 15);
    const char *artifact_version_val = PQgetisnull(res, 0, 16) ? NULL : PQgetvalue(res, 0, 16);
    const char *selection_count_val = PQgetisnull(res, 0, 17) ? NULL : PQgetvalue(res, 0, 17);
    long long artifact_size_num = 0;
    if (artifact_size_val) {
        artifact_size_num = atoll(artifact_size_val);
    }
    long long selected_count_num = 0;
    if (selection_count_val) {
        selected_count_num = atoll(selection_count_val);
    }
    bool download_ready = status_val && strcmp(status_val, "completed") == 0 && artifact_size_num > 0;

    Buffer buf;
    if (!buffer_init(&buf)) {
        if (error_out && !*error_out) {
            *error_out = strdup("Out of memory");
        }
        PQclear(res);
        return NULL;
    }

    if (!buffer_append_cstr(&buf, "{")) goto fail;
    if (!buffer_append_cstr(&buf, "\"job_id\":")) goto fail;
    if (!buffer_append_json_string(&buf, job_id_val ? job_id_val : "")) goto fail;
    if (!buffer_append_cstr(&buf, ",\"status\":")) goto fail;
    if (!buffer_append_json_string(&buf, status_val ? status_val : "unknown")) goto fail;
    if (!buffer_append_cstr(&buf, ",\"address\":")) goto fail;
    if (address_val) {
        if (!buffer_append_json_string(&buf, address_val)) goto fail;
    } else {
        if (!buffer_append_cstr(&buf, "null")) goto fail;
    }

    if (!buffer_append_cstr(&buf, ",\"created_at\":")) goto fail;
    if (created_val) {
        if (!buffer_append_json_string(&buf, created_val)) goto fail;
    } else {
        if (!buffer_append_cstr(&buf, "null")) goto fail;
    }

    if (!buffer_append_cstr(&buf, ",\"started_at\":")) goto fail;
    if (started_val) {
        if (!buffer_append_json_string(&buf, started_val)) goto fail;
    } else {
        if (!buffer_append_cstr(&buf, "null")) goto fail;
    }

    if (!buffer_append_cstr(&buf, ",\"completed_at\":")) goto fail;
    if (completed_val) {
        if (!buffer_append_json_string(&buf, completed_val)) goto fail;
    } else {
        if (!buffer_append_cstr(&buf, "null")) goto fail;
    }

    if (!buffer_append_cstr(&buf, ",\"error\":")) goto fail;
    if (error_val) {
        if (!buffer_append_json_string(&buf, error_val)) goto fail;
    } else {
        if (!buffer_append_cstr(&buf, "null")) goto fail;
    }

    if (!buffer_append_cstr(&buf, ",\"download_ready\":")) goto fail;
    if (!buffer_append_cstr(&buf, download_ready ? "true" : "false")) goto fail;

    if (!buffer_append_cstr(&buf, ",\"deficiency_only\":")) goto fail;
    if (!buffer_append_cstr(&buf, deficiency_only_val && (strcmp(deficiency_only_val, "t") == 0 || strcmp(deficiency_only_val, "true") == 0 || strcmp(deficiency_only_val, "1") == 0) ? "true" : "false")) goto fail;

    if (!buffer_append_cstr(&buf, ",\"job_type\":")) goto fail;
    if (job_type_val && job_type_val[0] != '\0') {
        if (!buffer_append_json_string(&buf, job_type_val)) goto fail;
    } else {
        if (!buffer_append_json_string(&buf, "audit")) goto fail;
    }

    if (!buffer_append_cstr(&buf, ",\"range_start\":")) goto fail;
    if (range_start_val && range_start_val[0] != '\0') {
        if (!buffer_append_json_string(&buf, range_start_val)) goto fail;
    } else {
        if (!buffer_append_cstr(&buf, "null")) goto fail;
    }

    if (!buffer_append_cstr(&buf, ",\"range_end\":")) goto fail;
    if (range_end_val && range_end_val[0] != '\0') {
        if (!buffer_append_json_string(&buf, range_end_val)) goto fail;
    } else {
        if (!buffer_append_cstr(&buf, "null")) goto fail;
    }

    if (!buffer_append_cstr(&buf, ",\"range_preset\":")) goto fail;
    if (range_preset_val && range_preset_val[0] != '\0') {
        if (!buffer_append_json_string(&buf, range_preset_val)) goto fail;
    } else {
        if (!buffer_append_cstr(&buf, "null")) goto fail;
    }

    if (!buffer_append_cstr(&buf, ",\"include_all\":")) goto fail;
    if (!buffer_append_cstr(&buf, include_all_val && (strcmp(include_all_val, "f") == 0 || strcmp(include_all_val, "false") == 0 || strcmp(include_all_val, "0") == 0) ? "false" : "true")) goto fail;

    if (!buffer_append_cstr(&buf, ",\"location_id\":")) goto fail;
    if (location_id_val && location_id_val[0] != '\0') {
        if (!buffer_append_json_string(&buf, location_id_val)) goto fail;
    } else {
        if (!buffer_append_cstr(&buf, "null")) goto fail;
    }

    if (!buffer_append_cstr(&buf, ",\"artifact_size\":")) goto fail;
    if (artifact_size_val) {
        if (!buffer_append_cstr(&buf, artifact_size_val)) goto fail;
    } else {
        if (!buffer_append_cstr(&buf, "null")) goto fail;
    }

    if (!buffer_append_cstr(&buf, ",\"artifact_filename\":")) goto fail;
    if (artifact_filename_val) {
        if (!buffer_append_json_string(&buf, artifact_filename_val)) goto fail;
    } else {
        if (!buffer_append_cstr(&buf, "null")) goto fail;
    }

    if (!buffer_append_cstr(&buf, ",\"selected_audit_count\":")) goto fail;
    if (!buffer_appendf(&buf, "%lld", selected_count_num)) goto fail;

    if (!buffer_append_cstr(&buf, ",\"version\":")) goto fail;
    if (artifact_version_val && artifact_version_val[0] != '\0') {
        if (!buffer_append_cstr(&buf, artifact_version_val)) goto fail;
    } else {
        if (!buffer_append_cstr(&buf, "null")) goto fail;
    }

    if (!buffer_append_cstr(&buf, ",\"download_url\":")) goto fail;
    if (download_ready && job_id_val) {
        Buffer url_buf;
        if (!buffer_init(&url_buf)) goto fail;
        const char *prefix = (path_prefix && path_prefix[0]) ? path_prefix : "";
        if (!buffer_append_cstr(&url_buf, prefix)) { buffer_free(&url_buf); goto fail; }
        if (!buffer_append_cstr(&url_buf, "/reports/")) { buffer_free(&url_buf); goto fail; }
        if (!buffer_append_cstr(&url_buf, job_id_val)) { buffer_free(&url_buf); goto fail; }
        if (!buffer_append_cstr(&url_buf, "/download")) { buffer_free(&url_buf); goto fail; }
        if (!buffer_append_json_string(&buf, url_buf.data ? url_buf.data : "")) { buffer_free(&url_buf); goto fail; }
        buffer_free(&url_buf);
    } else {
        if (!buffer_append_cstr(&buf, "null")) goto fail;
    }

    if (!buffer_append_cstr(&buf, "}")) goto fail;

    PQclear(res);
    char *result = buf.data;
    buf.data = NULL;
    buffer_free(&buf);
    return result;

fail:
    buffer_free(&buf);
    PQclear(res);
    if (error_out && !*error_out) {
        *error_out = strdup("Out of memory");
    }
    return NULL;
}

int db_find_existing_report_job(PGconn *conn,
                                const ReportJob *job,
                                char **job_id_out,
                                char **status_out,
                                bool *artifact_ready_out,
                                char **error_out) {
    if (job_id_out) *job_id_out = NULL;
    if (status_out) *status_out = NULL;
    if (artifact_ready_out) *artifact_ready_out = false;
    if (!conn || !job) {
        if (error_out && !*error_out) {
            *error_out = strdup("Report job context required");
        }
        return -1;
    }
    if (!job->include_all) {
        return 0;
    }
    if (!job->has_location_id && (!job->address || job->address[0] == '\0')) {
        if (error_out && !*error_out) {
            *error_out = strdup("Address or location id required");
        }
        return -1;
    }

    char location_buf[32];
    const char *location_param = NULL;
    if (job->has_location_id) {
        snprintf(location_buf, sizeof(location_buf), "%d", job->location_id);
        location_param = location_buf;
    }

    const char *job_type_param = report_job_type_to_string(job->type);
    const char *def_param = job->deficiency_only ? "true" : "false";
    const char *address_param = job->address ? job->address : "";
    const char *range_start_param = job->range_start;
    const char *range_end_param = job->range_end;
    const char *range_preset_param = job->range_preset;

    const char *params[7] = {
        job_type_param,
        def_param,
        location_param,
        address_param,
        range_start_param,
        range_end_param,
        range_preset_param
    };

    const char *base_where =
        " job_type = $1 "
        " AND deficiency_only = $2 "
        " AND include_all = true "
        " AND (( $3::int IS NOT NULL AND location_id = $3::int ) "
        "      OR ($3::int IS NULL AND address = $4)) "
        " AND COALESCE(range_start::text, '') = COALESCE($5::date::text, '') "
        " AND COALESCE(range_end::text, '') = COALESCE($6::date::text, '') "
        " AND COALESCE(range_preset, '') = COALESCE($7, '') ";

    char active_sql[1024];
    int active_len = snprintf(active_sql, sizeof(active_sql),
                              "SELECT job_id::text, status "
                              "FROM report_jobs "
                              "WHERE%s "
                              "AND status IN ('queued','processing') "
                              "ORDER BY created_at DESC NULLS LAST LIMIT 1",
                              base_where);
    if (active_len < 0 || (size_t)active_len >= sizeof(active_sql)) {
        if (error_out && !*error_out) {
            *error_out = strdup("Failed to build job lookup query");
        }
        return -1;
    }

    PGresult *res = PQexecParams(conn, active_sql, 7, NULL, params, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (error_out && !*error_out) {
            const char *msg = PQresultErrorMessage(res);
            *error_out = strdup(msg ? msg : "Failed to query report jobs");
        }
        PQclear(res);
        return -1;
    }

    if (PQntuples(res) > 0) {
        const char *job_id = PQgetvalue(res, 0, 0);
        const char *status = PQgetvalue(res, 0, 1);
        if (job_id_out) *job_id_out = strdup(job_id);
        if (status_out) *status_out = strdup(status ? status : "queued");
        if (artifact_ready_out) *artifact_ready_out = false;
        PQclear(res);
        return 1;
    }
    PQclear(res);

    char completed_sql[1024];
    int completed_len = snprintf(completed_sql, sizeof(completed_sql),
                                 "SELECT job_id::text, status, artifact_size "
                                 "FROM report_jobs "
                                 "WHERE%s "
                                 "AND status = 'completed' "
                                 "AND artifact_size IS NOT NULL "
                                 "ORDER BY completed_at DESC NULLS LAST LIMIT 1",
                                 base_where);
    if (completed_len < 0 || (size_t)completed_len >= sizeof(completed_sql)) {
        if (error_out && !*error_out) {
            *error_out = strdup("Failed to build job lookup query");
        }
        return -1;
    }
    res = PQexecParams(conn, completed_sql, 7, NULL, params, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (error_out && !*error_out) {
            const char *msg = PQresultErrorMessage(res);
            *error_out = strdup(msg ? msg : "Failed to query report jobs");
        }
        PQclear(res);
        return -1;
    }

    if (PQntuples(res) > 0) {
        const char *job_id = PQgetvalue(res, 0, 0);
        const char *status = PQgetvalue(res, 0, 1);
        const char *artifact_size_val = PQgetisnull(res, 0, 2) ? NULL : PQgetvalue(res, 0, 2);
        long long artifact_size_num = artifact_size_val ? atoll(artifact_size_val) : 0;
        if (job_id_out) *job_id_out = strdup(job_id);
        if (status_out) *status_out = strdup(status ? status : "completed");
        if (artifact_ready_out) *artifact_ready_out = artifact_size_num > 0;
        PQclear(res);
        return 1;
    }
    PQclear(res);
    return 0;
}
