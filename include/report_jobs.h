#ifndef REPORT_JOBS_H
#define REPORT_JOBS_H

#include <libpq-fe.h>
#include <stdbool.h>
#include <stddef.h>

#include "util.h"

typedef enum {
    REPORT_JOB_TYPE_AUDIT = 0,
    REPORT_JOB_TYPE_LOCATION_OVERVIEW
} ReportJobType;

typedef struct {
    char job_id[37];
    char *address;
    char *notes;
    char *recommendations;
    char *cover_building_owner;
    char *cover_street;
    char *cover_city;
    char *cover_state;
    char *cover_zip;
    char *cover_contact_name;
    char *cover_contact_email;
    ReportJobType type;
    bool deficiency_only;
    bool include_all;
    bool has_location_id;
    int location_id;
    char *range_start;
    char *range_end;
    char *range_preset;
    StringArray audit_ids;
} ReportJob;

void report_job_init(ReportJob *job);
void report_job_clear(ReportJob *job);

int db_insert_report_job(PGconn *conn, const char *job_id, const ReportJob *job, char **error_out);
int db_claim_next_report_job(PGconn *conn, ReportJob *job, char **error_out);
int db_complete_report_job(PGconn *conn,
                           const char *job_id,
                           const char *status,
                           const char *error_text,
                           const char *artifact_filename,
                           const char *artifact_mime,
                           const unsigned char *artifact_bytes,
                           size_t artifact_size,
                           char **error_out);
char *db_fetch_report_job_status(PGconn *conn, const char *job_id, const char *path_prefix, char **error_out);
int db_find_existing_report_job(PGconn *conn,
                                const ReportJob *job,
                                char **job_id_out,
                                char **status_out,
                                bool *artifact_ready_out,
                                char **error_out);

#endif /* REPORT_JOBS_H */
