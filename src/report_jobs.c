#include "report_jobs.h"

#include "buffer.h"
#include "log.h"

#include <stdbool.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

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
    job->deficiency_only = false;
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
    job->deficiency_only = false;
}

int db_insert_report_job(PGconn *conn, const char *job_id, const ReportJob *job, char **error_out) {
    if (!conn || !job_id || !job || !job->address || job->address[0] == '\0') {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid report job parameters");
        }
        return 0;
    }
    const char *sql =
        "INSERT INTO report_jobs (job_id, address, notes, recommendations, "
        "cover_building_owner, cover_street, cover_city, cover_state, cover_zip, cover_contact_name, cover_contact_email, deficiency_only) "
        "VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
    const char *params[12] = {
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
        job->deficiency_only ? "true" : "false"
    };
    PGresult *res = PQexecParams(conn, sql, 12, NULL, params, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        if (error_out && !*error_out) {
            const char *msg = PQresultErrorMessage(res);
            *error_out = strdup(msg ? msg : "Failed to insert report job");
        }
        PQclear(res);
        return 0;
    }
    PQclear(res);
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
        "           cover_building_owner, cover_street, cover_city, cover_state, cover_zip, cover_contact_name, cover_contact_email, deficiency_only "
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
        "          job.cover_building_owner, job.cover_street, job.cover_city, job.cover_state, job.cover_zip, job.cover_contact_name, job.cover_contact_email, job.deficiency_only";
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
    PQclear(res);
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
        "        SELECT id, address, deficiency_only "
        "        FROM report_jobs "
        "        WHERE job_id = $1::uuid"
        "    ), version_calc AS ("
        "        SELECT target.id, "
        "               CASE WHEN $2 = 'completed' THEN "
        "                    COALESCE(("
        "                        SELECT MAX(artifact_version) "
        "                        FROM report_jobs "
        "                        WHERE address = target.address "
        "                          AND deficiency_only = target.deficiency_only"
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
        "SELECT job_id::text, status, address, "
        "       to_char(created_at, 'YYYY-MM-DD" "T" "HH24:MI:SSOF'), "
        "       to_char(started_at, 'YYYY-MM-DD" "T" "HH24:MI:SSOF'), "
        "       to_char(completed_at, 'YYYY-MM-DD" "T" "HH24:MI:SSOF'), "
        "       error, "
        "       deficiency_only, "
        "       artifact_size, "
        "       artifact_filename, "
        "       artifact_version "
        "FROM report_jobs "
        "WHERE job_id = $1::uuid";
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
    const char *artifact_size_val = PQgetisnull(res, 0, 8) ? NULL : PQgetvalue(res, 0, 8);
    const char *artifact_filename_val = PQgetisnull(res, 0, 9) ? NULL : PQgetvalue(res, 0, 9);
    const char *artifact_version_val = PQgetisnull(res, 0, 10) ? NULL : PQgetvalue(res, 0, 10);
    long long artifact_size_num = 0;
    if (artifact_size_val) {
        artifact_size_num = atoll(artifact_size_val);
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
                                const char *address,
                                bool deficiency_only,
                                char **job_id_out,
                                char **status_out,
                                bool *artifact_ready_out,
                                char **error_out) {
    if (job_id_out) *job_id_out = NULL;
    if (status_out) *status_out = NULL;
    if (artifact_ready_out) *artifact_ready_out = false;
    if (!conn || !address) {
        if (error_out && !*error_out) {
            *error_out = strdup("Address required");
        }
        return -1;
    }

    const char *params[2] = { address, deficiency_only ? "true" : "false" };
    const char *active_sql =
        "SELECT job_id::text, status "
        "FROM report_jobs "
        "WHERE address = $1 AND deficiency_only = $2 AND status IN ('queued','processing') "
        "ORDER BY created_at DESC "
        "LIMIT 1";
    PGresult *res = PQexecParams(conn, active_sql, 2, NULL, params, NULL, NULL, 0);
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

    const char *completed_sql =
        "SELECT job_id::text, status, artifact_size "
        "FROM report_jobs "
        "WHERE address = $1 AND deficiency_only = $2 AND status = 'completed' AND artifact_size IS NOT NULL "
        "ORDER BY completed_at DESC NULLS LAST "
        "LIMIT 1";
    res = PQexecParams(conn, completed_sql, 2, NULL, params, NULL, NULL, 0);
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
