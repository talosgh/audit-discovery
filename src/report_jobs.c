#include "report_jobs.h"

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

char *db_fetch_report_job_status(PGconn *conn, const char *job_id, char **error_out) {
    if (!conn || !job_id) {
        if (error_out && !*error_out) {
            *error_out = strdup("Job id required");
        }
        return NULL;
    }
    const char *sql =
        "SELECT json_build_object("
        "  'job_id', job_id::text,"
        "  'status', status,"
        "  'address', address,"
        "  'created_at', created_at,"
        "  'started_at', started_at,"
        "  'completed_at', completed_at,"
        "  'error', error,"
        "  'download_ready', (status = 'completed' AND output_path IS NOT NULL)"
        ")::text "
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
    if (PQntuples(res) == 0 || PQgetisnull(res, 0, 0)) {
        PQclear(res);
        if (error_out && !*error_out) {
            *error_out = strdup("Report job not found");
        }
        return NULL;
    }
    const char *value = PQgetvalue(res, 0, 0);
    char *json = strdup(value ? value : "{}");
    PQclear(res);
    return json;
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
