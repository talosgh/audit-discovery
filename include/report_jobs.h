#ifndef REPORT_JOBS_H
#define REPORT_JOBS_H

#include <libpq-fe.h>

typedef struct {
    char job_id[37];
    char *address;
    char *notes;
    char *recommendations;
} ReportJob;

void report_job_init(ReportJob *job);
void report_job_clear(ReportJob *job);

int db_insert_report_job(PGconn *conn, const char *job_id, const char *address, const char *notes, const char *recs, char **error_out);
int db_claim_next_report_job(PGconn *conn, ReportJob *job, char **error_out);
int db_complete_report_job(PGconn *conn, const char *job_id, const char *status, const char *error_text, const char *output_path, char **error_out);
char *db_fetch_report_job_status(PGconn *conn, const char *job_id, const char *path_prefix, char **error_out);
int db_fetch_report_download_path(PGconn *conn, const char *job_id, char **path_out, char **error_out);
int db_find_existing_report_job(PGconn *conn, const char *address, char **job_id_out, char **status_out, char **output_path_out, char **error_out);

#endif /* REPORT_JOBS_H */
