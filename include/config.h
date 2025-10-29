#ifndef CONFIG_H
#define CONFIG_H

#include <stddef.h>

extern char *g_api_key;
extern char *g_api_prefix;
extern size_t g_api_prefix_len;
extern char *g_static_dir;
extern char *g_database_dsn;
extern char *g_report_output_dir;
extern char *g_report_assets_dir;
extern char *g_xai_api_key;
extern char *g_google_api_key;
extern char *g_google_region_code;

int load_env_file(const char *path);

#endif /* CONFIG_H */
