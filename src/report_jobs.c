#include "report_jobs.h"

#include "buffer.h"
#include "log.h"

#include <stdbool.h>
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
}

void report_job_clear(ReportJob *job) {
    if (!job) {
        return;
    }
    free(job->address);
    free(job->notes);
    free(job->recommendations);
    job->address = NULL;
    job->notes = NULL;
    job->recommendations = NULL;
    job->job_id[0] = '\0';
}

int db_insert_report_job(PGconn *conn, const char *job_id, const char *address, const char *notes, const char *recs, char **error_out) {
    if (!conn || !job_id || !address || address[0] == '\0') {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid report job parameters");
        }
        return 0;
    }
    const char *sql =
        "INSERT INTO report_jobs (job_id, address, notes, recommendations) "
        "VALUES ($1::uuid, $2, $3, $4)";
    const char *params[4] = { job_id, address, notes, recs };
    PGresult *res = PQexecParams(conn, sql, 4, NULL, params, NULL, NULL, 0);
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
        "    SELECT id, job_id::text AS job_id_text, address, notes, recommendations "
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
        "RETURNING job.job_id_text, job.address, job.notes, job.recommendations";
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
    PQclear(res);
    return 1;
}

int db_complete_report_job(PGconn *conn, const char *job_id, const char *status, const char *error_text, const char *output_path, char **error_out) {
    if (!conn || !job_id || !status) {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid report job completion parameters");
        }
        return 0;
    }
    const char *sql =
        "UPDATE report_jobs "
        "SET status = $2, "
        "    error = $3, "
        "    output_path = $4, "
        "    completed_at = CASE WHEN $2 IN ('completed','failed') THEN NOW() ELSE completed_at END, "
        "    updated_at = NOW() "
        "WHERE job_id = $1::uuid";
    const char *params[4] = { job_id, status, error_text, output_path };
    PGresult *res = PQexecParams(conn, sql, 4, NULL, params, NULL, NULL, 0);
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
        "       error, output_path "
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
    const char *output_path = PQgetisnull(res, 0, 7) ? NULL : PQgetvalue(res, 0, 7);
    bool download_ready = status_val && strcmp(status_val, "completed") == 0 && output_path && output_path[0] != '\0';

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

int db_fetch_report_download_path(PGconn *conn, const char *job_id, char **path_out, char **error_out) {
    if (!conn || !job_id || !path_out) {
        if (error_out && !*error_out) {
            *error_out = strdup("Job id required");
        }
        return 0;
    }
    const char *sql =
        "SELECT status, output_path "
        "FROM report_jobs "
        "WHERE job_id = $1::uuid";
    const char *params[1] = { job_id };
    PGresult *res = PQexecParams(conn, sql, 1, NULL, params, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (error_out && !*error_out) {
            const char *msg = PQresultErrorMessage(res);
            *error_out = strdup(msg ? msg : "Failed to fetch download path");
        }
        PQclear(res);
        return 0;
    }
    if (PQntuples(res) == 0) {
        if (error_out && !*error_out) {
            *error_out = strdup("Report job not found");
        }
        PQclear(res);
        return 0;
    }
    const char *status = PQgetvalue(res, 0, 0);
    bool completed = status && strcmp(status, "completed") == 0;
    if (!completed) {
        if (error_out && !*error_out) {
            *error_out = strdup("Report not ready");
        }
        PQclear(res);
        return 0;
    }
    if (PQgetisnull(res, 0, 1)) {
        if (error_out && !*error_out) {
            *error_out = strdup("Report artifact missing");
        }
        PQclear(res);
        return 0;
    }
    const char *path = PQgetvalue(res, 0, 1);
    *path_out = strdup(path);
    PQclear(res);
    if (!*path_out) {
        if (error_out && !*error_out) {
            *error_out = strdup("Out of memory copying path");
        }
        return 0;
    }
    return 1;
}

int db_find_existing_report_job(PGconn *conn, const char *address, char **job_id_out, char **status_out, char **output_path_out, char **error_out) {
    if (job_id_out) *job_id_out = NULL;
    if (status_out) *status_out = NULL;
    if (output_path_out) *output_path_out = NULL;
    if (!conn || !address) {
        if (error_out && !*error_out) {
            *error_out = strdup("Address required");
        }
        return -1;
    }

    const char *params[1] = { address };
    const char *active_sql =
        "SELECT job_id::text, status, output_path "
        "FROM report_jobs "
        "WHERE address = $1 AND status IN ('queued','processing') "
        "ORDER BY created_at DESC "
        "LIMIT 1";
    PGresult *res = PQexecParams(conn, active_sql, 1, NULL, params, NULL, NULL, 0);
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
        const char *output_path = PQgetisnull(res, 0, 2) ? NULL : PQgetvalue(res, 0, 2);
        if (job_id_out) *job_id_out = strdup(job_id);
        if (status_out) *status_out = strdup(status ? status : "queued");
        if (output_path_out && output_path) *output_path_out = strdup(output_path);
        PQclear(res);
        return 1;
    }
    PQclear(res);

    const char *completed_sql =
        "SELECT job_id::text, status, output_path "
        "FROM report_jobs "
        "WHERE address = $1 AND status = 'completed' AND output_path IS NOT NULL "
        "ORDER BY completed_at DESC NULLS LAST "
        "LIMIT 1";
    res = PQexecParams(conn, completed_sql, 1, NULL, params, NULL, NULL, 0);
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
        const char *output_path = PQgetisnull(res, 0, 2) ? NULL : PQgetvalue(res, 0, 2);
        if (job_id_out) *job_id_out = strdup(job_id);
        if (status_out) *status_out = strdup(status ? status : "completed");
        if (output_path_out && output_path) *output_path_out = strdup(output_path);
        PQclear(res);
        return 1;
    }
    PQclear(res);
    return 0;
}
