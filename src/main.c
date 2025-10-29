#include <arpa/inet.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <libpq-fe.h>
#include <limits.h>
#include <sys/sendfile.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <curl/curl.h>

#include "buffer.h"
#include "config.h"
#include "csv.h"
#include "db_helpers.h"
#include "fsutil.h"
#include "http.h"
#include "json.h"
#include "log.h"
#include "routes.h"
#include "report_jobs.h"
#include "server.h"
#include "text_utils.h"
#include "narrative.h"
#include "util.h"

#define DEFAULT_PORT 8080
#define MAX_HEADER_SIZE 65536
#define READ_BUFFER_SIZE 8192
#define TEMP_DIR_TEMPLATE  "/tmp/audit_unpack_XXXXXX"
static pthread_t g_report_thread;
static pthread_mutex_t g_report_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_report_cond = PTHREAD_COND_INITIALIZER;
static bool g_report_stop = false;
static bool g_report_signal = false;
static bool g_report_thread_started = false;
static bool g_curl_initialized = false;

typedef struct {
    bool has_value;
    int value;
} OptionalInt;

typedef struct {
    bool has_value;
    long value;
} OptionalLong;

typedef struct {
    bool has_value;
    double value;
} OptionalDouble;

typedef struct {
    bool has_value;
    bool value;
} OptionalBool;

typedef struct {
    int section_counter;
    char *violation_device_id;
    char *equipment_code;
    char *condition_code;
    char *remedy_code;
    char *overlay_code;
    char *violation_equipment;
    char *violation_condition;
    char *violation_remedy;
    char *violation_note;
} Deficiency;

typedef struct {
    Deficiency *items;
    size_t count;
    size_t capacity;
} DeficiencyList;

typedef struct {
    char *filename;
    char *content_type;
    unsigned char *data;
    size_t size;
} PhotoFile;

typedef struct {
    PhotoFile *items;
    size_t count;
    size_t capacity;
} PhotoCollection;

typedef struct {
    char **items;
    size_t count;
    size_t capacity;
} AllocationList;

typedef struct {
    char *equipment;
    char *condition;
    char *remedy;
    char *note;
    char *condition_code_raw;
    OptionalBool resolved;
    char *resolved_at;
} ReportDeficiency;

typedef struct {
    ReportDeficiency *items;
    size_t count;
    size_t capacity;
} ReportDeficiencyList;

typedef struct {
    char *key;
    int count;
} KeyCountEntry;

typedef struct {
    KeyCountEntry *items;
    size_t count;
    size_t capacity;
} KeyCountList;

typedef struct {
    char *device_id;
    StringArray codes;
} DeviceCodesEntry;

typedef struct {
    DeviceCodesEntry *items;
    size_t count;
    size_t capacity;
} DeviceCodesList;

typedef struct {
    OptionalInt controller_install_year;
    OptionalInt controller_age;
    OptionalInt capacity;
    OptionalInt car_speed;
    OptionalInt number_of_stops;
    OptionalInt number_of_openings;
    OptionalInt code_data_year;
    OptionalInt ride_quality;
    OptionalDouble door_opening_width;
    OptionalBool dlm_compliant;
    OptionalBool maintenance_log_up_to_date;
    OptionalBool cat1_tag_current;
    OptionalBool cat5_tag_current;
    OptionalBool code_data_plate_present;
    OptionalBool is_first_car;
} ReportDeviceMetrics;

typedef struct {
    char *audit_uuid;
    char *device_id;
    char *submission_id;
    char *device_type;
    char *bank_name;
    char *city_id;
    char *general_notes;
    char *controller_manufacturer;
    char *controller_model;
    char *controller_type;
    char *controller_power_system;
    char *machine_manufacturer;
    char *machine_type;
    char *roping;
    char *door_operation;
    char *door_operation_type;
    char *cat1_tag_date;
    char *cat5_tag_date;
    char *submitted_on_iso;
    StringArray floors_served;
    StringArray cars_in_bank;
    StringArray total_floor_stop_names;
    ReportDeviceMetrics metrics;
    ReportDeficiencyList deficiencies;
    const char *docstring;
    const char *deficiencies_docstring;
} ReportDevice;

typedef struct {
    ReportDevice *items;
    size_t count;
    size_t capacity;
} ReportDeviceList;

typedef struct {
    char *start;
    char *end;
} ReportDateRange;

typedef struct {
    const char *docstring;
    char *building_address;
    char *building_owner;
    char *elevator_contractor;
    char *city_id;
    ReportDateRange audit_range;
    int total_devices;
    int elevator_count;
    int escalator_count;
    int audit_count;
    int total_deficiencies;
    double average_deficiencies_per_device;
    KeyCountList deficiencies_by_code;
} ReportSummary;

typedef struct {
    ReportSummary summary;
    ReportDeviceList devices;
    DeviceCodesList deficiency_codes_by_device;
} ReportData;

static const char *REPORT_SUMMARY_DOCSTRING =
    "Contains high-level metrics about the elevator audit.\n"
    "- total_deficiencies: Total number of deficiencies/violations across all devices.\n"
    "- total_devices: Total number of unique devices audited.\n"
    "- deficiencies_by_code: Count of deficiencies per condition code (e.g., RUBBING, UNGUARDED).\n"
    "- total_deficiencies_per_device: Number of deficiencies per device.\n"
    "- average_deficiencies_per_device: Average number of deficiencies across all devices.\n"
    "- audit_date_range: Earliest and latest submission dates.\n"
    "- building_address: Address of the building audited.\n";

static const char *REPORT_DEVICE_DOCSTRING =
    "Contains detailed information for a single elevator device.\n"
    "- device_id: Unique identifier for the device (normalized from Building ID).\n"
    "- device_type: 'elevator' to distinguish from escalator devices.\n"
    "- submission_id: Original submission UUID.\n"
    "- root_details: All columns from the Root sheet for this device.\n"
    "- general_notes: Inspector's general notes about this device.\n"
    "- deficiencies: List of deficiencies with their details.\n"
    "- deficiencies_docstring: Description of the deficiency fields.\n"
    "- ride_quality: Ride quality rating (from CI_VARIABLES).\n"
    "- controller_age: Age of the controller in years.\n"
    "- dlm_compliant: Whether the device is Door Lock Monitoring compliant.\n"
    "- unintended_motion_compliant: Whether the device meets unintended motion requirements.\n"
    "- code_data_year: The revision year of the ASME A17.1 code that governs this elevator.\n"
    "- cat1_tag_up_to_date: Whether the Category 1 test tag is current and shows testing compliance.\n"
    "- cat5_tag_up_to_date: Whether the Category 5 test tag is current and shows testing compliance.\n"
    "- maintenance_log_up_to_date: Whether the maintenance log is current and properly maintained.\n";

static const char *REPORT_DEFICIENCIES_DOCSTRING =
    "Each deficiency includes:\n"
    "- equipment: The equipment related to the deficiency (e.g., GOVERNOR ROPES).\n"
    "- condition: The condition causing the deficiency (e.g., RUBBING).\n"
    "- remedy: The recommended remedy for the deficiency (e.g., ADJUST).\n"
    "- note: Additional notes or description of the deficiency.\n";

static const char *SUMMARY_DEF_PER_DEVICE_DOCSTRING =
    "Number of deficiencies per device.\n"
    "- Key: Device ID\n"
    "- Value: Number of deficiencies for that device\n";

static const char *SUMMARY_DEF_CODES_BY_DEVICE_DOCSTRING =
    "List of deficiency condition codes per device.\n"
    "- Key: Device ID\n"
    "- Value: List of deficiency condition codes for that device\n";

typedef struct {
    char *audit_uuid;
    OptionalLong form_id;
    char *form_name;
    OptionalInt form_version;
    char *submitted_on; // ISO 8601 string
    char *submitted_by;
    char *updated_at;
    OptionalLong account_id;
    OptionalLong user_id;
    char *user_name;
    char *submit_guid;
    OptionalInt rating_overall;
    char *workflow_stage;
    char *workflow_user;

    char *building_address;
    char *building_owner;
    char *elevator_contractor;
    char *city_id;
    char *building_id;
    char *device_type;
    OptionalBool is_first_car;
    char *building_information;
    char *bank_name;
    StringArray cars_in_bank;
    StringArray total_floor_stop_names;
    StringArray floors_served;

    char *machine_room_location;
    char *machine_room_location_other;
    char *controller_manufacturer;
    char *controller_model;
    OptionalInt controller_install_year;
    char *controller_type;
    char *controller_power_system;
    OptionalInt car_speed;
    OptionalBool dlm_compliant;
    OptionalBool maintenance_log_up_to_date;
    char *last_maintenance_log_date;
    OptionalBool code_data_plate_present;
    OptionalInt code_data_year;
    OptionalBool cat1_tag_current;
    char *cat1_tag_date;
    OptionalBool cat5_tag_current;
    char *cat5_tag_date;
    OptionalBool brake_tag_current;
    char *brake_tag_date;

    char *machine_manufacturer;
    char *machine_type;
    OptionalInt number_of_ropes;
    char *roping;
    OptionalInt rope_condition_score;
    OptionalBool motor_data_plate_present;
    char *motor_type;
    char *brake_type;
    char *single_or_dual_core_brake;
    OptionalBool rope_gripper_present;
    char *governor_manufacturer;
    char *governor_type;
    OptionalBool counterweight_governor;
    char *pump_motor_manufacturer;
    char *oil_condition;
    char *oil_level;
    char *valve_manufacturer;
    OptionalBool tank_heater_present;
    OptionalBool oil_cooler_present;
    OptionalInt capacity;
    char *door_operation;
    char *door_operation_type;
    OptionalInt number_of_openings;
    OptionalInt number_of_stops;
    char *pi_type;
    char *rail_type;
    char *guide_type;
    char *car_door_equipment_manufacturer;
    char *car_door_lock_manufacturer;
    char *car_door_operator_manufacturer;
    char *car_door_operator_model;
    char *restrictor_type;
    OptionalBool has_hoistway_access_keyswitches;
    char *hallway_pi_type;
    char *hatch_door_unlocking_type;
    char *hatch_door_equipment_manufacturer;
    char *hatch_door_lock_manufacturer;
    char *pit_access;
    char *safety_type;
    char *buffer_type;
    OptionalBool sump_pump_present;
    char *compensation_type;
    char *jack_piston_type;
    OptionalBool scavenger_pump_present;
    char *general_notes;
    OptionalDouble door_opening_width;
    OptionalInt expected_stop_count;

    char *mobile_device;
    char *mobile_app_name;
    char *mobile_app_version;
    char *mobile_app_type;
    char *mobile_sdk_release;
    OptionalLong mobile_memory_mb;
} AuditRecord;

typedef struct {
    char *executive_summary;
    char *key_findings;
    char *methodology;
    char *maintenance_performance;
    char *recommendations;
    char *conclusion;
} NarrativeSet;

static void handle_options_request(int client_fd);
static void handle_client(int client_fd, void *ctx);
static const char *optional_bool_to_text(const OptionalBool *value);
static const char *optional_int_to_text(const OptionalInt *value, char *buffer, size_t buffer_len);
static char *build_deficiency_key(const char *overlay_code, const char *device_id, const char *equipment, const char *condition, const char *remedy, const char *note);
static OptionalInt parse_optional_int(const char *text);
static OptionalDouble parse_optional_double(const char *text);
static OptionalBool parse_optional_bool(const char *text);
static int assign_string(char **dest, const char *value);
static void report_data_init(ReportData *data);
static void report_data_clear(ReportData *data);
static void report_device_init(ReportDevice *device);
static void report_device_clear(ReportDevice *device);
static int report_device_list_append_move(ReportDeviceList *list, ReportDevice *device);
static char *build_location_detail_json(const ReportData *report);
static int load_report_for_building(PGconn *conn, const char *building_address, ReportData *report, char **error_out);
static char *report_data_to_json(const ReportData *report);
static char *build_location_detail_payload(PGconn *conn, const char *address, int *status_out, char **error_out);
static char *build_report_json_payload(PGconn *conn, const char *address, int *status_out, char **error_out);
static char *build_download_url(const char *job_id);
static void send_report_job_response(int client_fd, int http_status, const char *status_value, const char *job_id, const char *address_value, const char *download_url);
static int generate_uuid_v4(char out[37]);
static int process_report_job(PGconn *conn, const ReportJob *job, char **output_path_out, char **error_out);
static void *report_worker_main(void *arg);
static void signal_report_worker(void);
static void narrative_set_init(NarrativeSet *set);
static void narrative_set_clear(NarrativeSet *set);
static int build_report_latex(const ReportData *report, const NarrativeSet *narratives, const ReportJob *job, const char *output_path, char **error_out);
static int run_pdflatex(const char *working_dir, const char *tex_filename, char **error_out);
static bool read_request_body(int client_fd,
                              const char *header_lines,
                              char *body_start,
                              size_t leftover,
                              char saved_body_char,
                              long max_length,
                              char **body_out,
                              long *length_out,
                              int *status_out,
                              const char **error_out);
static char *create_temp_dir(void);
static int process_extracted_archive(char *temp_dir, PGconn *conn, StringArray *processed_audits, char **error_out);
static bool handle_zip_upload(int client_fd,
                              char *body_start,
                              size_t leftover,
                              char saved_body_char,
                              long content_length,
                              PGconn *conn,
                              StringArray *processed_audits,
                              int *status_out,
                              char **error_out);
static int export_building_photos(PGconn *conn, const ReportData *report, const char *root_dir, char **error_out);
static char *sanitize_path_component(const char *input);
static int copy_file_contents(const char *src_path, const char *dst_path);
static int create_report_archive(PGconn *conn, const ReportJob *job, const ReportData *report,
                                 const char *job_dir, const char *pdf_path, char **zip_path_out, char **error_out);
typedef struct {
    const char *system_prompt;
    char *prompt;
    char **slot;
    char *error;
    int success;
    pthread_t thread;
    bool thread_started;
} NarrativeTask;

static void narrative_task_execute(NarrativeTask *task);
static void *narrative_thread_main(void *arg);
static bool read_request_body(int client_fd,
                              const char *header_lines,
                              char *body_start,
                              size_t leftover,
                              char saved_body_char,
                              long max_length,
                              char **body_out,
                              long *length_out,
                              int *status_out,
                              const char **error_out);

static void optional_int_clear(OptionalInt *value) { value->has_value = false; value->value = 0; }
static void optional_long_clear(OptionalLong *value) { value->has_value = false; value->value = 0; }
static void optional_double_clear(OptionalDouble *value) { value->has_value = false; value->value = 0.0; }
static void optional_bool_clear(OptionalBool *value) { value->has_value = false; value->value = false; }









static const char *optional_bool_to_text(const OptionalBool *value) {
    if (!value || !value->has_value) {
        return "—";
    }
    return value->value ? "Yes" : "No";
}

static const char *optional_int_to_text(const OptionalInt *value, char *buffer, size_t buffer_len) {
    if (!buffer || buffer_len == 0) {
        return "";
    }
    if (!value || !value->has_value) {
        return "—";
    }
    snprintf(buffer, buffer_len, "%d", value->value);
    return buffer;
}


static void narrative_set_init(NarrativeSet *set) {
    if (!set) return;
    set->executive_summary = NULL;
    set->key_findings = NULL;
    set->methodology = NULL;
    set->maintenance_performance = NULL;
    set->recommendations = NULL;
    set->conclusion = NULL;
}

static void narrative_set_clear(NarrativeSet *set) {
    if (!set) return;
    free(set->executive_summary);
    free(set->key_findings);
    free(set->methodology);
    free(set->maintenance_performance);
    free(set->recommendations);
    free(set->conclusion);
    narrative_set_init(set);
}

static int generate_uuid_v4(char out[37]) {
    if (!out) {
        return 0;
    }
    unsigned char bytes[16];
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd >= 0) {
        ssize_t n = read(fd, bytes, sizeof(bytes));
        close(fd);
        if (n != (ssize_t)sizeof(bytes)) {
            fd = -1;
        }
    }
    if (fd < 0) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        unsigned int seed = (unsigned int)(ts.tv_nsec ^ ts.tv_sec ^ getpid());
        for (size_t i = 0; i < sizeof(bytes); ++i) {
            seed = seed * 1103515245u + 12345u;
            bytes[i] = (unsigned char)((seed >> 16) & 0xFF);
        }
    }
    bytes[6] = (unsigned char)((bytes[6] & 0x0F) | 0x40);
    bytes[8] = (unsigned char)((bytes[8] & 0x3F) | 0x80);
    int written = snprintf(out, 37,
                           "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                           bytes[0], bytes[1], bytes[2], bytes[3],
                           bytes[4], bytes[5],
                           bytes[6], bytes[7],
                           bytes[8], bytes[9],
                           bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]);
    return written == 36;
}

typedef struct {
    char *key;
    char *resolved_at;
} ResolvedEntry;

typedef struct {
    ResolvedEntry *entries;
    size_t count;
    size_t capacity;
} ResolvedMap;

static void resolved_map_init(ResolvedMap *map) {
    map->entries = NULL;
    map->count = 0;
    map->capacity = 0;
}

static void resolved_map_clear(ResolvedMap *map) {
    if (!map) return;
    for (size_t i = 0; i < map->count; ++i) {
        free(map->entries[i].key);
        free(map->entries[i].resolved_at);
    }
    free(map->entries);
    map->entries = NULL;
    map->count = 0;
    map->capacity = 0;
}

static int resolved_map_put(ResolvedMap *map, const char *key, const char *resolved_at) {
    if (!key) {
        return 1;
    }
    for (size_t i = 0; i < map->count; ++i) {
        if (strcmp(map->entries[i].key, key) == 0) {
            if (!map->entries[i].resolved_at && resolved_at) {
                map->entries[i].resolved_at = strdup(resolved_at);
                if (!map->entries[i].resolved_at) {
                    return 0;
                }
            }
            return 1;
        }
    }
    if (map->count == map->capacity) {
        size_t new_cap = map->capacity == 0 ? 8 : map->capacity * 2;
        ResolvedEntry *tmp = realloc(map->entries, new_cap * sizeof(ResolvedEntry));
        if (!tmp) {
            return 0;
        }
        map->entries = tmp;
        map->capacity = new_cap;
    }
    map->entries[map->count].key = strdup(key);
    map->entries[map->count].resolved_at = resolved_at ? strdup(resolved_at) : NULL;
    if (!map->entries[map->count].key || (resolved_at && !map->entries[map->count].resolved_at)) {
        free(map->entries[map->count].key);
        free(map->entries[map->count].resolved_at);
        return 0;
    }
    map->count++;
    return 1;
}

static const char *resolved_map_get(const ResolvedMap *map, const char *key) {
    if (!map || !key) {
        return NULL;
    }
    for (size_t i = 0; i < map->count; ++i) {
        if (strcmp(map->entries[i].key, key) == 0) {
            return map->entries[i].resolved_at;
        }
    }
    return NULL;
}

static char *strdup_safe(const char *value) {
    if (!value) {
        return NULL;
    }
    return strdup(value);
}

static void report_deficiency_list_init(ReportDeficiencyList *list) {
    list->items = NULL;
    list->count = 0;
    list->capacity = 0;
}

static void report_deficiency_free(ReportDeficiency *def) {
    if (!def) return;
    free(def->equipment);
    free(def->condition);
    free(def->remedy);
    free(def->note);
    free(def->condition_code_raw);
    free(def->resolved_at);
    optional_bool_clear(&def->resolved);
}

static void report_deficiency_list_clear(ReportDeficiencyList *list) {
    if (!list) return;
    for (size_t i = 0; i < list->count; ++i) {
        report_deficiency_free(&list->items[i]);
    }
    free(list->items);
    list->items = NULL;
    list->count = 0;
    list->capacity = 0;
}

static int report_deficiency_list_append(ReportDeficiencyList *list,
                                         const char *equipment,
                                         const char *condition,
                                         const char *remedy,
                                         const char *note,
                                         const char *condition_raw,
                                         const char *resolved_at,
                                         OptionalBool resolved_flag) {
    if (!list) {
        return 0;
    }
    if (list->count == list->capacity) {
        size_t new_cap = list->capacity == 0 ? 4 : list->capacity * 2;
        ReportDeficiency *tmp = realloc(list->items, new_cap * sizeof(ReportDeficiency));
        if (!tmp) {
            return 0;
        }
        list->items = tmp;
        list->capacity = new_cap;
    }
    ReportDeficiency *def = &list->items[list->count];
    memset(def, 0, sizeof(*def));

    def->equipment = strdup_safe(equipment);
    def->condition = strdup_safe(condition);
    def->remedy = strdup_safe(remedy);
    def->note = strdup_safe(note);
    def->condition_code_raw = strdup_safe(condition_raw);
    def->resolved_at = strdup_safe(resolved_at);
    def->resolved = resolved_flag;

    if ((equipment && !def->equipment) ||
        (condition && !def->condition) ||
        (remedy && !def->remedy) ||
        (note && !def->note) ||
        (condition_raw && !def->condition_code_raw) ||
        (resolved_at && !def->resolved_at)) {
        report_deficiency_free(def);
        return 0;
    }

    list->count++;
    return 1;
}

static void key_count_list_init(KeyCountList *list) {
    list->items = NULL;
    list->count = 0;
    list->capacity = 0;
}

static void key_count_list_clear(KeyCountList *list) {
    if (!list) return;
    for (size_t i = 0; i < list->count; ++i) {
        free(list->items[i].key);
    }
    free(list->items);
    list->items = NULL;
    list->count = 0;
    list->capacity = 0;
}

static int key_count_list_increment(KeyCountList *list, const char *key, int delta) {
    if (!list) return 0;
    const char *effective = (key && key[0]) ? key : "Unspecified";
    for (size_t i = 0; i < list->count; ++i) {
        if (strcmp(list->items[i].key, effective) == 0) {
            list->items[i].count += delta;
            return 1;
        }
    }
    if (list->count == list->capacity) {
        size_t new_cap = list->capacity == 0 ? 8 : list->capacity * 2;
        KeyCountEntry *tmp = realloc(list->items, new_cap * sizeof(KeyCountEntry));
        if (!tmp) {
            return 0;
        }
        list->items = tmp;
        list->capacity = new_cap;
    }
    list->items[list->count].key = strdup(effective);
    if (!list->items[list->count].key) {
        return 0;
    }
    list->items[list->count].count = delta;
    list->count++;
    return 1;
}

static void device_codes_list_init(DeviceCodesList *list) {
    list->items = NULL;
    list->count = 0;
    list->capacity = 0;
}

static void device_codes_list_clear(DeviceCodesList *list) {
    if (!list) return;
    for (size_t i = 0; i < list->count; ++i) {
        free(list->items[i].device_id);
        string_array_clear(&list->items[i].codes);
    }
    free(list->items);
    list->items = NULL;
    list->count = 0;
    list->capacity = 0;
}

static StringArray *device_codes_list_get(DeviceCodesList *list, const char *device_id, bool create) {
    if (!list || !device_id) return NULL;
    for (size_t i = 0; i < list->count; ++i) {
        if (strcmp(list->items[i].device_id, device_id) == 0) {
            return &list->items[i].codes;
        }
    }
    if (!create) {
        return NULL;
    }
    if (list->count == list->capacity) {
        size_t new_cap = list->capacity == 0 ? 4 : list->capacity * 2;
        DeviceCodesEntry *tmp = realloc(list->items, new_cap * sizeof(DeviceCodesEntry));
        if (!tmp) {
            return NULL;
        }
        list->items = tmp;
        list->capacity = new_cap;
    }
    DeviceCodesEntry *entry = &list->items[list->count];
    entry->device_id = strdup(device_id);
    if (!entry->device_id) {
        return NULL;
    }
    string_array_init(&entry->codes);
    list->count++;
    return &entry->codes;
}

static int json_text_to_string_array(const char *json_text, StringArray *out) {
    if (!out) return 0;
    if (!json_text || json_text[0] == '\0') {
        return 1;
    }
    char *error = NULL;
    JsonValue *value = json_parse(json_text, &error);
    if (!value) {
        if (error) {
            log_error("Failed to parse JSON array: %s", error);
            free(error);
        }
        return 0;
    }
    if (value->type != JSON_ARRAY) {
        json_free(value);
        return 1;
    }
    for (size_t i = 0; i < value->value.array.count; ++i) {
        const JsonValue *item = value->value.array.items[i];
        const char *str = json_as_string(item);
        if (str && !string_array_append_copy(out, str)) {
            json_free(value);
            return 0;
        }
    }
    json_free(value);
    return 1;
}

static void assign_optional_int_from_pg(OptionalInt *out, PGresult *res, int row, int col) {
    if (!out) return;
    optional_int_clear(out);
    if (!res || PQgetisnull(res, row, col)) {
        return;
    }
    const char *value = PQgetvalue(res, row, col);
    OptionalInt parsed = parse_optional_int(value);
    if (parsed.has_value) {
        *out = parsed;
    }
}

static void assign_optional_double_from_pg(OptionalDouble *out, PGresult *res, int row, int col) {
    if (!out) return;
    optional_double_clear(out);
    if (!res || PQgetisnull(res, row, col)) {
        return;
    }
    const char *value = PQgetvalue(res, row, col);
    OptionalDouble parsed = parse_optional_double(value);
    if (parsed.has_value) {
        *out = parsed;
    }
}

static void assign_optional_bool_from_pg(OptionalBool *out, PGresult *res, int row, int col) {
    if (!out) return;
    optional_bool_clear(out);
    if (!res || PQgetisnull(res, row, col)) {
        return;
    }
    const char *value = PQgetvalue(res, row, col);
    OptionalBool parsed = parse_optional_bool(value);
    if (!parsed.has_value) {
        if (strcasecmp(value, "t") == 0) {
            parsed.has_value = true;
            parsed.value = true;
        } else if (strcasecmp(value, "f") == 0) {
            parsed.has_value = true;
            parsed.value = false;
        }
    }
    if (parsed.has_value) {
        *out = parsed;
    }
}

static OptionalInt calculate_controller_age_from_year(const OptionalInt *install_year) {
    OptionalInt result;
    optional_int_clear(&result);
    if (!install_year || !install_year->has_value) {
        return result;
    }
    time_t now = time(NULL);
    struct tm tm_now;
    if (!localtime_r(&now, &tm_now)) {
        return result;
    }
    int current_year = tm_now.tm_year + 1900;
    result.has_value = true;
    result.value = current_year - install_year->value;
    return result;
}

static char *clean_deficiency_text(const char *input) {
    if (!input) {
        return NULL;
    }
    while (isspace((unsigned char)*input)) {
        input++;
    }
    if (*input == '\0') {
        return strdup("");
    }
    const char *start = input;
    const char *dash = strchr(input, '-');
    if (dash) {
        const char *p = input;
        bool matches = true;
        while (p < dash) {
            if (*p == ' ') {
                p++;
                continue;
            }
            if (!(isupper((unsigned char)*p) || isdigit((unsigned char)*p))) {
                matches = false;
                break;
            }
            p++;
        }
        if (matches) {
            start = dash + 1;
            while (isspace((unsigned char)*start)) {
                start++;
            }
        }
    }
    char *result = strdup(start);
    if (!result) {
        return NULL;
    }
    for (size_t i = 0; result[i]; ++i) {
        result[i] = (char)(i == 0 ? toupper((unsigned char)result[i]) : tolower((unsigned char)result[i]));
    }
    return result;
}

static int assign_string_from_pg(char **dest, PGresult *res, int row, int col) {
    if (!dest) return 0;
    if (!res || PQgetisnull(res, row, col)) {
        return 1;
    }
    return assign_string(dest, PQgetvalue(res, row, col));
}

static int buffer_append_optional_int(Buffer *buf, const OptionalInt *value) {
    if (!value || !value->has_value) {
        return buffer_append_cstr(buf, "null");
    }
    return buffer_appendf(buf, "%d", value->value);
}

static int buffer_append_optional_double(Buffer *buf, const OptionalDouble *value) {
    if (!value || !value->has_value) {
        return buffer_append_cstr(buf, "null");
    }
    return buffer_appendf(buf, "%g", value->value);
}

static int buffer_append_optional_bool(Buffer *buf, const OptionalBool *value) {
    if (!value || !value->has_value) {
        return buffer_append_cstr(buf, "null");
    }
    return buffer_append_cstr(buf, value->value ? "true" : "false");
}

static int buffer_append_string_array(Buffer *buf, const StringArray *array) {
    if (!buffer_append_char(buf, '[')) return 0;
    if (array && array->count > 0) {
        for (size_t i = 0; i < array->count; ++i) {
            if (i > 0 && !buffer_append_char(buf, ',')) return 0;
            if (!buffer_append_json_string(buf, array->values[i])) return 0;
        }
    }
    if (!buffer_append_char(buf, ']')) return 0;
    return 1;
}

static char *build_location_detail_json(const ReportData *report) {
    if (!report) {
        return NULL;
    }

    Buffer buf;
    if (!buffer_init(&buf)) {
        return NULL;
    }

    if (!buffer_append_char(&buf, '{')) goto oom;
    if (!buffer_append_cstr(&buf, "\"summary\":{")) goto oom;
    if (!buffer_append_cstr(&buf, "\"address\":")) goto oom;
    if (!buffer_append_json_string(&buf, report->summary.building_address)) goto oom;
    if (!buffer_append_char(&buf, ',')) goto oom;

    if (!buffer_append_cstr(&buf, "\"building_owner\":")) goto oom;
    if (!buffer_append_json_string(&buf, report->summary.building_owner)) goto oom;
    if (!buffer_append_char(&buf, ',')) goto oom;

    if (!buffer_append_cstr(&buf, "\"elevator_contractor\":")) goto oom;
    if (!buffer_append_json_string(&buf, report->summary.elevator_contractor)) goto oom;
    if (!buffer_append_char(&buf, ',')) goto oom;

    if (!buffer_append_cstr(&buf, "\"city_id\":")) goto oom;
    if (!buffer_append_json_string(&buf, report->summary.city_id)) goto oom;
    if (!buffer_append_char(&buf, ',')) goto oom;

    if (!buffer_append_cstr(&buf, "\"device_count\":")) goto oom;
    if (!buffer_appendf(&buf, "%d", report->summary.total_devices)) goto oom;
    if (!buffer_append_char(&buf, ',')) goto oom;

    if (!buffer_append_cstr(&buf, "\"audit_count\":")) goto oom;
    if (!buffer_appendf(&buf, "%d", report->summary.audit_count)) goto oom;
    if (!buffer_append_char(&buf, ',')) goto oom;

    if (!buffer_append_cstr(&buf, "\"first_audit\":")) goto oom;
    if (!buffer_append_json_string(&buf, report->summary.audit_range.start)) goto oom;
    if (!buffer_append_char(&buf, ',')) goto oom;

    if (!buffer_append_cstr(&buf, "\"last_audit\":")) goto oom;
    if (!buffer_append_json_string(&buf, report->summary.audit_range.end)) goto oom;
    if (!buffer_append_char(&buf, ',')) goto oom;

    if (!buffer_append_cstr(&buf, "\"total_deficiencies\":")) goto oom;
    if (!buffer_appendf(&buf, "%d", report->summary.total_deficiencies)) goto oom;
    if (!buffer_append_char(&buf, ',')) goto oom;

    size_t total_open = 0;
    for (size_t i = 0; i < report->devices.count; ++i) {
        const ReportDevice *device = &report->devices.items[i];
        for (size_t j = 0; j < device->deficiencies.count; ++j) {
            const ReportDeficiency *def = &device->deficiencies.items[j];
            bool resolved = def->resolved.has_value ? def->resolved.value : false;
            if (!resolved) {
                total_open++;
            }
        }
    }

    if (!buffer_append_cstr(&buf, "\"open_deficiencies\":")) goto oom;
    if (!buffer_appendf(&buf, "%zu", total_open)) goto oom;
    if (!buffer_append_char(&buf, ',')) goto oom;

    if (!buffer_append_cstr(&buf, "\"deficiencies_by_code\":{")) goto oom;
    for (size_t i = 0; i < report->summary.deficiencies_by_code.count; ++i) {
        if (i > 0 && !buffer_append_char(&buf, ',')) goto oom;
        if (!buffer_append_json_string(&buf, report->summary.deficiencies_by_code.items[i].key)) goto oom;
        if (!buffer_append_char(&buf, ':')) goto oom;
        if (!buffer_appendf(&buf, "%d", report->summary.deficiencies_by_code.items[i].count)) goto oom;
    }
    if (!buffer_append_char(&buf, '}')) goto oom;
    if (!buffer_append_char(&buf, '}')) goto oom; // close summary

    if (!buffer_append_cstr(&buf, ",\"devices\":[")) goto oom;
    for (size_t i = 0; i < report->devices.count; ++i) {
        const ReportDevice *device = &report->devices.items[i];
        size_t device_total = device->deficiencies.count;
        size_t device_open = 0;
        for (size_t j = 0; j < device->deficiencies.count; ++j) {
            const ReportDeficiency *def = &device->deficiencies.items[j];
            bool resolved = def->resolved.has_value ? def->resolved.value : false;
            if (!resolved) {
                device_open++;
            }
        }

        if (i > 0 && !buffer_append_char(&buf, ',')) goto oom;
        if (!buffer_append_char(&buf, '{')) goto oom;

        if (!buffer_append_cstr(&buf, "\"audit_uuid\":")) goto oom;
        if (!buffer_append_json_string(&buf, device->audit_uuid ? device->audit_uuid : device->submission_id)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"device_id\":")) goto oom;
        if (!buffer_append_json_string(&buf, device->device_id)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"device_type\":")) goto oom;
        if (!buffer_append_json_string(&buf, device->device_type)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"bank_name\":")) goto oom;
        if (!buffer_append_json_string(&buf, device->bank_name)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"city_id\":")) goto oom;
        if (!buffer_append_json_string(&buf, device->city_id)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"submitted_on\":")) goto oom;
        if (!buffer_append_json_string(&buf, device->submitted_on_iso)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"controller_install_year\":")) goto oom;
        if (!buffer_append_optional_int(&buf, &device->metrics.controller_install_year)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"controller_age\":")) goto oom;
        if (!buffer_append_optional_int(&buf, &device->metrics.controller_age)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"dlm_compliant\":")) goto oom;
        if (!buffer_append_optional_bool(&buf, &device->metrics.dlm_compliant)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"cat1_tag_current\":")) goto oom;
        if (!buffer_append_optional_bool(&buf, &device->metrics.cat1_tag_current)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"cat5_tag_current\":")) goto oom;
        if (!buffer_append_optional_bool(&buf, &device->metrics.cat5_tag_current)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"maintenance_log_up_to_date\":")) goto oom;
        if (!buffer_append_optional_bool(&buf, &device->metrics.maintenance_log_up_to_date)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"is_first_car\":")) goto oom;
        if (!buffer_append_optional_bool(&buf, &device->metrics.is_first_car)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"total_deficiencies\":")) goto oom;
        if (!buffer_appendf(&buf, "%zu", device_total)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"open_deficiencies\":")) goto oom;
        if (!buffer_appendf(&buf, "%zu", device_open)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"cars_in_bank\":")) goto oom;
        if (!buffer_append_string_array(&buf, &device->cars_in_bank)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"floors_served\":")) goto oom;
        if (!buffer_append_string_array(&buf, &device->floors_served)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"total_floor_stop_names\":")) goto oom;
        if (!buffer_append_string_array(&buf, &device->total_floor_stop_names)) goto oom;
        if (!buffer_append_char(&buf, ',')) goto oom;

        if (!buffer_append_cstr(&buf, "\"deficiencies\":[")) goto oom;
        for (size_t j = 0; j < device->deficiencies.count; ++j) {
            const ReportDeficiency *def = &device->deficiencies.items[j];
            if (j > 0 && !buffer_append_char(&buf, ',')) goto oom;
            if (!buffer_append_char(&buf, '{')) goto oom;
            if (!buffer_append_cstr(&buf, "\"equipment\":")) goto oom;
            if (!buffer_append_json_string(&buf, def->equipment)) goto oom;
            if (!buffer_append_char(&buf, ',')) goto oom;
            if (!buffer_append_cstr(&buf, "\"condition\":")) goto oom;
            if (!buffer_append_json_string(&buf, def->condition)) goto oom;
            if (!buffer_append_char(&buf, ',')) goto oom;
            if (!buffer_append_cstr(&buf, "\"remedy\":")) goto oom;
            if (!buffer_append_json_string(&buf, def->remedy)) goto oom;
            if (!buffer_append_char(&buf, ',')) goto oom;
            if (!buffer_append_cstr(&buf, "\"note\":")) goto oom;
            if (!buffer_append_json_string(&buf, def->note)) goto oom;
            if (!buffer_append_char(&buf, ',')) goto oom;
            if (!buffer_append_cstr(&buf, "\"resolved\":")) goto oom;
            {
                bool resolved = def->resolved.has_value ? def->resolved.value : false;
                if (!buffer_append_cstr(&buf, resolved ? "true" : "false")) goto oom;
            }
            if (!buffer_append_char(&buf, ',')) goto oom;
            if (!buffer_append_cstr(&buf, "\"resolved_at\":")) goto oom;
            if (!buffer_append_json_string(&buf, def->resolved_at)) goto oom;
            if (!buffer_append_char(&buf, '}')) goto oom;
        }
        if (!buffer_append_char(&buf, ']')) goto oom;
        if (!buffer_append_char(&buf, '}')) goto oom;
    }
    if (!buffer_append_char(&buf, ']')) goto oom;
    if (!buffer_append_char(&buf, '}')) goto oom;

    char *json = buf.data;
    buf.data = NULL;
    buffer_free(&buf);
    return json;

oom:
    buffer_free(&buf);
    return NULL;
}

static int load_deficiencies_for_audit(PGconn *conn,
                                       const char *audit_uuid,
                                       ReportData *report,
                                       ReportDevice *device,
                                       DeviceCodesList *codes_map,
                                       char **error_out) {
    if (!conn || !audit_uuid || !report || !device) {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid parameters while loading deficiencies");
        }
        return 0;
    }
    const char *sql =
        "SELECT "
        "  violation_equipment,"
        "  violation_condition,"
        "  violation_remedy,"
        "  violation_note,"
        "  condition_code,"
        "  resolved_at"
        " FROM audit_deficiencies"
        " WHERE audit_uuid = $1::uuid"
        " ORDER BY id";
    const char *params[1] = { audit_uuid };
    PGresult *res = PQexecParams(conn, sql, 1, NULL, params, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (error_out && !*error_out) {
            const char *msg = PQresultErrorMessage(res);
            *error_out = strdup(msg ? msg : "Failed to query deficiencies");
        }
        PQclear(res);
        return 0;
    }

    DeviceCodesList local_map;
    DeviceCodesList *codes_dst = codes_map ? codes_map : &local_map;
    if (!codes_map) {
        device_codes_list_init(&local_map);
    }

    for (int row = 0; row < PQntuples(res); ++row) {
        const char *raw_equipment = PQgetisnull(res, row, 0) ? NULL : PQgetvalue(res, row, 0);
        const char *raw_condition = PQgetisnull(res, row, 1) ? NULL : PQgetvalue(res, row, 1);
        const char *raw_remedy = PQgetisnull(res, row, 2) ? NULL : PQgetvalue(res, row, 2);
        const char *raw_note = PQgetisnull(res, row, 3) ? NULL : PQgetvalue(res, row, 3);
        const char *raw_condition_code = PQgetisnull(res, row, 4) ? NULL : PQgetvalue(res, row, 4);
        const char *resolved_at = PQgetisnull(res, row, 5) ? NULL : PQgetvalue(res, row, 5);

        char *clean_equipment = clean_deficiency_text(raw_equipment);
        char *clean_condition = clean_deficiency_text(raw_condition);
        char *clean_remedy = clean_deficiency_text(raw_remedy);

        OptionalBool resolved_flag;
        optional_bool_clear(&resolved_flag);
        resolved_flag.has_value = true;
        resolved_flag.value = false;
        if (resolved_at) {
            resolved_flag.has_value = true;
            resolved_flag.value = true;
        }

        if (!report_deficiency_list_append(&device->deficiencies,
                                           clean_equipment ? clean_equipment : raw_equipment,
                                           clean_condition ? clean_condition : raw_condition,
                                           clean_remedy ? clean_remedy : raw_remedy,
                                           raw_note,
                                           raw_condition_code,
                                           resolved_at,
                                           resolved_flag)) {
            if (error_out && !*error_out) {
                *error_out = strdup("Failed to append deficiency");
            }
            free(clean_equipment);
            free(clean_condition);
            free(clean_remedy);
            if (!codes_map) device_codes_list_clear(&local_map);
            PQclear(res);
            return 0;
        }

        const char *condition_for_summary = clean_condition ? clean_condition : raw_condition;
        if (!key_count_list_increment(&report->summary.deficiencies_by_code, condition_for_summary, 1)) {
            if (error_out && !*error_out) {
                *error_out = strdup("Failed to update deficiency summary counts");
            }
            free(clean_equipment);
            free(clean_condition);
            free(clean_remedy);
            if (!codes_map) device_codes_list_clear(&local_map);
            PQclear(res);
            return 0;
        }

        report->summary.total_deficiencies += 1;

        if (device->device_id) {
            StringArray *code_list = device_codes_list_get(codes_dst, device->device_id, true);
            if (!code_list || !string_array_append_copy(code_list, condition_for_summary)) {
                if (error_out && !*error_out) {
                    *error_out = strdup("Failed to record deficiency code list");
                }
                free(clean_equipment);
                free(clean_condition);
                free(clean_remedy);
                if (!codes_map) device_codes_list_clear(&local_map);
                PQclear(res);
                return 0;
            }
        }

        free(clean_equipment);
        free(clean_condition);
        free(clean_remedy);
    }

    if (!codes_map) {
        device_codes_list_clear(&local_map);
    }
    PQclear(res);
    return 1;
}

static char *build_location_detail_payload(PGconn *conn, const char *address, int *status_out, char **error_out) {
    if (status_out) {
        *status_out = 500;
    }
    if (error_out) {
        *error_out = NULL;
    }
    if (!conn || !address || address[0] == '\0') {
        if (error_out && !*error_out) {
            *error_out = strdup("address field is required");
        }
        if (status_out) {
            *status_out = 400;
        }
        return NULL;
    }

    ReportData report;
    report_data_init(&report);
    char *load_error = NULL;
    int ok = load_report_for_building(conn, address, &report, &load_error);
    if (!ok) {
        int status = 500;
        if (load_error && strcmp(load_error, "No audits found for building address") == 0) {
            status = 404;
        }
        if (error_out && !*error_out) {
            *error_out = load_error ? load_error : strdup("Failed to load location detail");
            load_error = NULL;
        }
        free(load_error);
        report_data_clear(&report);
        if (status_out) {
            *status_out = status;
        }
        return NULL;
    }
    free(load_error);

    char *json = build_location_detail_json(&report);
    report_data_clear(&report);
    if (!json) {
        if (error_out && !*error_out) {
            *error_out = strdup("Failed to serialize location detail");
        }
        if (status_out) {
            *status_out = 500;
        }
        return NULL;
    }
    if (status_out) {
        *status_out = 200;
    }
    return json;
}

static char *build_report_json_payload(PGconn *conn, const char *address, int *status_out, char **error_out) {
    if (status_out) {
        *status_out = 500;
    }
    if (error_out) {
        *error_out = NULL;
    }
    if (!conn || !address || address[0] == '\0') {
        if (error_out && !*error_out) {
            *error_out = strdup("address field is required");
        }
        if (status_out) {
            *status_out = 400;
        }
        return NULL;
    }

    ReportData report;
    report_data_init(&report);
    char *load_error = NULL;
    int ok = load_report_for_building(conn, address, &report, &load_error);
    if (!ok) {
        int status = 500;
        if (load_error && strcmp(load_error, "No audits found for building address") == 0) {
            status = 404;
        }
        if (error_out && !*error_out) {
            *error_out = load_error ? load_error : strdup("Failed to load report");
            load_error = NULL;
        }
        free(load_error);
        report_data_clear(&report);
        if (status_out) {
            *status_out = status;
        }
        return NULL;
    }
    free(load_error);

    char *json = report_data_to_json(&report);
    report_data_clear(&report);
    if (!json) {
        if (error_out && !*error_out) {
            *error_out = strdup("Failed to serialize report");
        }
        if (status_out) {
            *status_out = 500;
        }
        return NULL;
    }
    if (status_out) {
        *status_out = 200;
    }
    return json;
}

static char *build_download_url(const char *job_id) {
    if (!job_id || !*job_id) {
        return NULL;
    }
    const char *prefix = (g_api_prefix && g_api_prefix[0]) ? g_api_prefix : "";
    const char reports_segment[] = "/reports/";
    const char download_segment[] = "/download";
    size_t prefix_len = strlen(prefix);
    size_t total = prefix_len + sizeof(reports_segment) - 1 + strlen(job_id) + sizeof(download_segment) - 1 + 1;
    char *url = malloc(total);
    if (!url) {
        return NULL;
    }
    size_t pos = 0;
    if (prefix_len > 0) {
        memcpy(url + pos, prefix, prefix_len);
        pos += prefix_len;
    }
    memcpy(url + pos, reports_segment, sizeof(reports_segment) - 1);
    pos += sizeof(reports_segment) - 1;
    size_t job_len = strlen(job_id);
    memcpy(url + pos, job_id, job_len);
    pos += job_len;
    memcpy(url + pos, download_segment, sizeof(download_segment) - 1);
    pos += sizeof(download_segment) - 1;
    url[pos] = '\0';
    return url;
}

static void send_report_job_response(int client_fd, int http_status, const char *status_value, const char *job_id, const char *address_value, const char *download_url) {
    Buffer buf;
    if (!buffer_init(&buf)) {
        char *body = build_error_response("Out of memory");
        send_http_json(client_fd, 500, "Internal Server Error", body);
        free(body);
        return;
    }

    if (!buffer_append_cstr(&buf, "{")) goto fail;
    if (!buffer_append_cstr(&buf, "\"status\":")) goto fail;
    if (!buffer_append_json_string(&buf, status_value ? status_value : "queued")) goto fail;
    if (!buffer_append_cstr(&buf, ",\"job_id\":")) goto fail;
    if (!buffer_append_json_string(&buf, job_id ? job_id : "")) goto fail;
    if (!buffer_append_cstr(&buf, ",\"address\":")) goto fail;
    if (address_value) {
        if (!buffer_append_json_string(&buf, address_value)) goto fail;
    } else {
        if (!buffer_append_cstr(&buf, "null")) goto fail;
    }
    if (!buffer_append_cstr(&buf, ",\"download_url\":")) goto fail;
    if (download_url && download_url[0]) {
        if (!buffer_append_json_string(&buf, download_url)) goto fail;
    } else {
        if (!buffer_append_cstr(&buf, "null")) goto fail;
    }
    if (!buffer_append_cstr(&buf, "}")) goto fail;

    {
        const char *status_text = (http_status == 200) ? "OK" : (http_status == 202 ? "Accepted" : "OK");
        send_http_json(client_fd, http_status, status_text, buf.data ? buf.data : "{}");
    }
    buffer_free(&buf);
    return;

fail:
    buffer_free(&buf);
    {
        char *body = build_error_response("Failed to build response");
        send_http_json(client_fd, 500, "Internal Server Error", body);
        free(body);
    }
}

static int load_report_for_building(PGconn *conn, const char *building_address, ReportData *report, char **error_out) {
    if (!conn || !building_address || !report) {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid parameters while loading report");
        }
        return 0;
    }

    const char *sql =
        "SELECT "
        "  a.audit_uuid::text,"
        "  a.building_id,"
        "  a.device_type,"
        "  a.bank_name,"
        "  a.general_notes,"
        "  a.controller_install_year,"
        "  a.controller_manufacturer,"
        "  a.controller_model,"
        "  a.controller_type,"
        "  a.controller_power_system,"
        "  a.machine_manufacturer,"
        "  a.machine_type,"
        "  a.roping,"
        "  a.door_operation,"
        "  a.door_operation_type,"
        "  a.number_of_stops,"
        "  a.number_of_openings,"
        "  a.capacity,"
        "  a.car_speed,"
        "  a.code_data_year,"
        "  a.cat1_tag_current,"
        "  a.cat1_tag_date,"
        "  a.cat5_tag_current,"
        "  a.cat5_tag_date,"
        "  a.dlm_compliant,"
        "  a.maintenance_log_up_to_date,"
        "  a.code_data_plate_present,"
        "  a.door_opening_width,"
        "  a.rating_overall,"
        "  a.submitted_on,"
        "  array_to_json(COALESCE(a.cars_in_bank, ARRAY[]::text[]))::text AS cars_in_bank_json,"
        "  array_to_json(COALESCE(a.total_floor_stop_names, ARRAY[]::text[]))::text AS total_floor_stop_names_json,"
        "  array_to_json(COALESCE(a.floors_served, ARRAY[]::text[]))::text AS floors_served_json,"
        "  a.building_address,"
        "  a.building_owner,"
        "  a.elevator_contractor,"
        "  a.city_id,"
        "  a.is_first_car"
        " FROM audits a"
        " WHERE a.building_address = $1"
        " ORDER BY a.submitted_on NULLS LAST, a.building_id";

    const char *params[1] = { building_address };
    PGresult *res = PQexecParams(conn, sql, 1, NULL, params, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (error_out && !*error_out) {
            const char *msg = PQresultErrorMessage(res);
            *error_out = strdup(msg ? msg : "Failed to query audits for building");
        }
        PQclear(res);
        return 0;
    }

    int rows = PQntuples(res);
    if (rows == 0) {
        if (error_out && !*error_out) {
            *error_out = strdup("No audits found for building address");
        }
        PQclear(res);
        return 0;
    }

    report_data_clear(report);
    report_data_init(report);
    report->summary.audit_count = rows;

    for (int row = 0; row < rows; ++row) {
        ReportDevice device;
        report_device_init(&device);

        if (!assign_string_from_pg(&device.audit_uuid, res, row, 0) ||
            !assign_string_from_pg(&device.submission_id, res, row, 0) ||
            !assign_string_from_pg(&device.device_id, res, row, 1) ||
            !assign_string_from_pg(&device.device_type, res, row, 2) ||
            !assign_string_from_pg(&device.bank_name, res, row, 3) ||
            !assign_string_from_pg(&device.city_id, res, row, 36) ||
            !assign_string_from_pg(&device.general_notes, res, row, 4) ||
            !assign_string_from_pg(&device.controller_manufacturer, res, row, 6) ||
            !assign_string_from_pg(&device.controller_model, res, row, 7) ||
            !assign_string_from_pg(&device.controller_type, res, row, 8) ||
            !assign_string_from_pg(&device.controller_power_system, res, row, 9) ||
            !assign_string_from_pg(&device.machine_manufacturer, res, row, 10) ||
            !assign_string_from_pg(&device.machine_type, res, row, 11) ||
            !assign_string_from_pg(&device.roping, res, row, 12) ||
            !assign_string_from_pg(&device.door_operation, res, row, 13) ||
            !assign_string_from_pg(&device.door_operation_type, res, row, 14) ||
            !assign_string_from_pg(&device.cat1_tag_date, res, row, 21) ||
            !assign_string_from_pg(&device.cat5_tag_date, res, row, 23) ||
            !assign_string_from_pg(&device.submitted_on_iso, res, row, 29)) {
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory while copying audit fields");
            }
            report_device_clear(&device);
            PQclear(res);
            return 0;
        }

        assign_optional_int_from_pg(&device.metrics.controller_install_year, res, row, 5);
        assign_optional_int_from_pg(&device.metrics.number_of_stops, res, row, 15);
        assign_optional_int_from_pg(&device.metrics.number_of_openings, res, row, 16);
        assign_optional_int_from_pg(&device.metrics.capacity, res, row, 17);
        assign_optional_int_from_pg(&device.metrics.car_speed, res, row, 18);
        assign_optional_int_from_pg(&device.metrics.code_data_year, res, row, 19);
        assign_optional_bool_from_pg(&device.metrics.cat1_tag_current, res, row, 20);
        assign_optional_bool_from_pg(&device.metrics.cat5_tag_current, res, row, 22);
        assign_optional_bool_from_pg(&device.metrics.dlm_compliant, res, row, 24);
        assign_optional_bool_from_pg(&device.metrics.maintenance_log_up_to_date, res, row, 25);
        assign_optional_bool_from_pg(&device.metrics.code_data_plate_present, res, row, 26);
        assign_optional_double_from_pg(&device.metrics.door_opening_width, res, row, 27);
        assign_optional_int_from_pg(&device.metrics.ride_quality, res, row, 28);
        assign_optional_bool_from_pg(&device.metrics.is_first_car, res, row, 37);

        device.metrics.controller_age = calculate_controller_age_from_year(&device.metrics.controller_install_year);

        const char *cars_json = PQgetisnull(res, row, 30) ? NULL : PQgetvalue(res, row, 30);
        const char *stops_json = PQgetisnull(res, row, 31) ? NULL : PQgetvalue(res, row, 31);
        const char *floors_json = PQgetisnull(res, row, 32) ? NULL : PQgetvalue(res, row, 32);
        if (!json_text_to_string_array(cars_json, &device.cars_in_bank) ||
            !json_text_to_string_array(stops_json, &device.total_floor_stop_names) ||
            !json_text_to_string_array(floors_json, &device.floors_served)) {
            if (error_out && !*error_out) {
                *error_out = strdup("Failed to parse audit array fields");
            }
            report_device_clear(&device);
            PQclear(res);
            return 0;
        }

        if (!report->summary.building_address && !assign_string_from_pg(&report->summary.building_address, res, row, 33)) {
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory copying building address");
            }
            report_device_clear(&device);
            PQclear(res);
            return 0;
        }
        if (!report->summary.building_owner && !assign_string_from_pg(&report->summary.building_owner, res, row, 34)) {
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory copying building owner");
            }
            report_device_clear(&device);
            PQclear(res);
            return 0;
        }
        if (!report->summary.elevator_contractor && !assign_string_from_pg(&report->summary.elevator_contractor, res, row, 35)) {
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory copying elevator contractor");
            }
            report_device_clear(&device);
            PQclear(res);
            return 0;
        }
        if (!report->summary.city_id && !assign_string_from_pg(&report->summary.city_id, res, row, 36)) {
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory copying city id");
            }
            report_device_clear(&device);
            PQclear(res);
            return 0;
        }

        if (!load_deficiencies_for_audit(conn,
                                          device.submission_id,
                                          report,
                                          &device,
                                          &report->deficiency_codes_by_device,
                                          error_out)) {
            report_device_clear(&device);
            PQclear(res);
            return 0;
        }

        if (!report_device_list_append_move(&report->devices, &device)) {
            if (error_out && !*error_out) {
                *error_out = strdup("Failed to append device to report");
            }
            report_device_clear(&device);
            PQclear(res);
            return 0;
        }

        report->summary.total_devices += 1;
        const char *type = report->devices.items[report->devices.count - 1].device_type;
        if (type && strcasecmp(type, "escalator") == 0) {
            report->summary.escalator_count += 1;
        } else {
            report->summary.elevator_count += 1;
        }

        const char *submitted_on = report->devices.items[report->devices.count - 1].submitted_on_iso;
        if (submitted_on) {
            if (!report->summary.audit_range.start || strcmp(submitted_on, report->summary.audit_range.start) < 0) {
                free(report->summary.audit_range.start);
                report->summary.audit_range.start = strdup(submitted_on);
            }
            if (!report->summary.audit_range.end || strcmp(submitted_on, report->summary.audit_range.end) > 0) {
                free(report->summary.audit_range.end);
                report->summary.audit_range.end = strdup(submitted_on);
            }
        }
    }

    PQclear(res);

    if (report->summary.total_devices > 0) {
        report->summary.average_deficiencies_per_device =
            (double)report->summary.total_deficiencies / (double)report->summary.total_devices;
    } else {
        report->summary.average_deficiencies_per_device = 0.0;
    }

    return 1;
}

static char *report_data_to_json(const ReportData *report) {
    if (!report) return NULL;
    Buffer buf;
    if (!buffer_init(&buf)) {
        return NULL;
    }

    if (!buffer_append_char(&buf, '{')) goto fail;

    if (!buffer_append_cstr(&buf, "\"summary\":{")) goto fail;
    if (!buffer_append_cstr(&buf, "\"docstring\":")) goto fail;
    if (!buffer_append_json_string(&buf, report->summary.docstring)) goto fail;
    if (!buffer_append_char(&buf, ',')) goto fail;

    if (!buffer_append_cstr(&buf, "\"building_address\":")) goto fail;
    if (!buffer_append_json_string(&buf, report->summary.building_address)) goto fail;
    if (!buffer_append_char(&buf, ',')) goto fail;

    if (!buffer_append_cstr(&buf, "\"building_owner\":")) goto fail;
    if (!buffer_append_json_string(&buf, report->summary.building_owner)) goto fail;
    if (!buffer_append_char(&buf, ',')) goto fail;

    if (!buffer_append_cstr(&buf, "\"elevator_contractor\":")) goto fail;
    if (!buffer_append_json_string(&buf, report->summary.elevator_contractor)) goto fail;
    if (!buffer_append_char(&buf, ',')) goto fail;

    if (!buffer_append_cstr(&buf, "\"city_id\":")) goto fail;
    if (!buffer_append_json_string(&buf, report->summary.city_id)) goto fail;
    if (!buffer_append_char(&buf, ',')) goto fail;

    if (!buffer_append_cstr(&buf, "\"total_devices\":")) goto fail;
    if (!buffer_appendf(&buf, "%d", report->summary.total_devices)) goto fail;
    if (!buffer_append_char(&buf, ',')) goto fail;

    if (!buffer_append_cstr(&buf, "\"elevator_count\":")) goto fail;
    if (!buffer_appendf(&buf, "%d", report->summary.elevator_count)) goto fail;
    if (!buffer_append_char(&buf, ',')) goto fail;

    if (!buffer_append_cstr(&buf, "\"escalator_count\":")) goto fail;
    if (!buffer_appendf(&buf, "%d", report->summary.escalator_count)) goto fail;
    if (!buffer_append_char(&buf, ',')) goto fail;

    if (!buffer_append_cstr(&buf, "\"audit_count\":")) goto fail;
    if (!buffer_appendf(&buf, "%d", report->summary.audit_count)) goto fail;
    if (!buffer_append_char(&buf, ',')) goto fail;

    if (!buffer_append_cstr(&buf, "\"total_deficiencies\":")) goto fail;
    if (!buffer_appendf(&buf, "%d", report->summary.total_deficiencies)) goto fail;
    if (!buffer_append_char(&buf, ',')) goto fail;

    if (!buffer_append_cstr(&buf, "\"average_deficiencies_per_device\":")) goto fail;
    if (!buffer_appendf(&buf, "%0.6f", report->summary.average_deficiencies_per_device)) goto fail;
    if (!buffer_append_char(&buf, ',')) goto fail;

    if (!buffer_append_cstr(&buf, "\"audit_date_range\":")) goto fail;
    if (report->summary.audit_range.start || report->summary.audit_range.end) {
        if (!buffer_append_char(&buf, '{')) goto fail;
        if (!buffer_append_cstr(&buf, "\"start\":")) goto fail;
        if (!buffer_append_json_string(&buf, report->summary.audit_range.start)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_cstr(&buf, "\"end\":")) goto fail;
        if (!buffer_append_json_string(&buf, report->summary.audit_range.end)) goto fail;
        if (!buffer_append_char(&buf, '}')) goto fail;
    } else {
        if (!buffer_append_cstr(&buf, "null")) goto fail;
    }
    if (!buffer_append_char(&buf, ',')) goto fail;

    if (!buffer_append_cstr(&buf, "\"deficiencies_by_code\":")) goto fail;
    if (!buffer_append_char(&buf, '{')) goto fail;
    for (size_t i = 0; i < report->summary.deficiencies_by_code.count; ++i) {
        if (i > 0 && !buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_json_string(&buf, report->summary.deficiencies_by_code.items[i].key)) goto fail;
        if (!buffer_append_char(&buf, ':')) goto fail;
        if (!buffer_appendf(&buf, "%d", report->summary.deficiencies_by_code.items[i].count)) goto fail;
    }
    if (!buffer_append_char(&buf, '}')) goto fail;
    if (!buffer_append_char(&buf, ',')) goto fail;

    if (!buffer_append_cstr(&buf, "\"total_deficiencies_per_device\":{")) goto fail;
    if (!buffer_append_cstr(&buf, "\"docstring\":")) goto fail;
    if (!buffer_append_json_string(&buf, SUMMARY_DEF_PER_DEVICE_DOCSTRING)) goto fail;
    for (size_t i = 0; i < report->devices.count; ++i) {
        const ReportDevice *device = &report->devices.items[i];
        if (!device->device_id) continue;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_json_string(&buf, device->device_id)) goto fail;
        if (!buffer_append_char(&buf, ':')) goto fail;
        if (!buffer_appendf(&buf, "%zu", device->deficiencies.count)) goto fail;
    }
    if (!buffer_append_char(&buf, '}')) goto fail;
    if (!buffer_append_char(&buf, ',')) goto fail;

    if (!buffer_append_cstr(&buf, "\"deficiency_codes_by_device\":{")) goto fail;
    if (!buffer_append_cstr(&buf, "\"docstring\":")) goto fail;
    if (!buffer_append_json_string(&buf, SUMMARY_DEF_CODES_BY_DEVICE_DOCSTRING)) goto fail;
    for (size_t i = 0; i < report->deficiency_codes_by_device.count; ++i) {
        DeviceCodesEntry *entry = &report->deficiency_codes_by_device.items[i];
        if (!entry->device_id) continue;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_json_string(&buf, entry->device_id)) goto fail;
        if (!buffer_append_char(&buf, ':')) goto fail;
        if (!buffer_append_string_array(&buf, &entry->codes)) goto fail;
    }
    if (!buffer_append_char(&buf, '}')) goto fail;
    if (!buffer_append_char(&buf, '}')) goto fail; // end summary

    if (!buffer_append_cstr(&buf, ",\"devices\":{")) goto fail;
    for (size_t i = 0; i < report->devices.count; ++i) {
        const ReportDevice *device = &report->devices.items[i];
        if (!device->device_id) continue;
        if (i > 0 && !buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_json_string(&buf, device->device_id)) goto fail;
        if (!buffer_append_cstr(&buf, ":{")) goto fail;

        if (!buffer_append_cstr(&buf, "\"docstring\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->docstring)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"device_id\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->device_id)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"device_type\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->device_type)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"submission_id\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->submission_id)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"bank_name\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->bank_name)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"submitted_on\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->submitted_on_iso)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"general_notes\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->general_notes)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"deficiencies_docstring\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->deficiencies_docstring)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"root_details\":{")) goto fail;
        if (!buffer_append_cstr(&buf, "\"Device Type\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->device_type)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_cstr(&buf, "\"Capacity\":")) goto fail;
        if (!buffer_append_optional_int(&buf, &device->metrics.capacity)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_cstr(&buf, "\"Car Speed\":")) goto fail;
        if (!buffer_append_optional_int(&buf, &device->metrics.car_speed)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_cstr(&buf, "\"Floors Served\":")) goto fail;
        if (!buffer_append_string_array(&buf, &device->floors_served)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_cstr(&buf, "\"Cars In Bank\":")) goto fail;
        if (!buffer_append_string_array(&buf, &device->cars_in_bank)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_cstr(&buf, "\"Total Building Floor Stop Names\":")) goto fail;
        if (!buffer_append_string_array(&buf, &device->total_floor_stop_names)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_cstr(&buf, "\"Controller Manufacturer\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->controller_manufacturer)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_cstr(&buf, "\"Controller Model\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->controller_model)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_cstr(&buf, "\"Controller Installation Year\":")) goto fail;
        if (!buffer_append_optional_int(&buf, &device->metrics.controller_install_year)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_cstr(&buf, "\"Machine Manufacturer\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->machine_manufacturer)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_cstr(&buf, "\"Machine Type\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->machine_type)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_cstr(&buf, "\"Roping\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->roping)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_cstr(&buf, "\"Door Operation\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->door_operation)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_cstr(&buf, "\"Door Operation Type\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->door_operation_type)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_cstr(&buf, "\"Number of Stops\":")) goto fail;
        if (!buffer_append_optional_int(&buf, &device->metrics.number_of_stops)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_cstr(&buf, "\"Number of Openings\":")) goto fail;
        if (!buffer_append_optional_int(&buf, &device->metrics.number_of_openings)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_cstr(&buf, "\"Code Data Year\":")) goto fail;
        if (!buffer_append_optional_int(&buf, &device->metrics.code_data_year)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_cstr(&buf, "\"Controller Type\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->controller_type)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;
        if (!buffer_append_cstr(&buf, "\"Controller Power System\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->controller_power_system)) goto fail;
        if (!buffer_append_char(&buf, '}')) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"deficiencies\":")) goto fail;
        if (!buffer_append_char(&buf, '[')) goto fail;
        for (size_t j = 0; j < device->deficiencies.count; ++j) {
            const ReportDeficiency *def = &device->deficiencies.items[j];
            if (j > 0 && !buffer_append_char(&buf, ',')) goto fail;
            if (!buffer_append_char(&buf, '{')) goto fail;
            if (!buffer_append_cstr(&buf, "\"equipment\":")) goto fail;
            if (!buffer_append_json_string(&buf, def->equipment)) goto fail;
            if (!buffer_append_char(&buf, ',')) goto fail;
            if (!buffer_append_cstr(&buf, "\"condition\":")) goto fail;
            if (!buffer_append_json_string(&buf, def->condition)) goto fail;
            if (!buffer_append_char(&buf, ',')) goto fail;
            if (!buffer_append_cstr(&buf, "\"remedy\":")) goto fail;
            if (!buffer_append_json_string(&buf, def->remedy)) goto fail;
            if (!buffer_append_char(&buf, ',')) goto fail;
            if (!buffer_append_cstr(&buf, "\"note\":")) goto fail;
            if (!buffer_append_json_string(&buf, def->note)) goto fail;
            if (!buffer_append_char(&buf, ',')) goto fail;
            if (!buffer_append_cstr(&buf, "\"resolved_at\":")) goto fail;
            if (!buffer_append_json_string(&buf, def->resolved_at)) goto fail;
            if (!buffer_append_char(&buf, '}')) goto fail;
        }
        if (!buffer_append_char(&buf, ']')) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"ride_quality\":")) goto fail;
        if (!buffer_append_optional_int(&buf, &device->metrics.ride_quality)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"controller_age\":")) goto fail;
        if (!buffer_append_optional_int(&buf, &device->metrics.controller_age)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"dlm_compliant\":")) goto fail;
        if (!buffer_append_optional_bool(&buf, &device->metrics.dlm_compliant)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"maintenance_log_up_to_date\":")) goto fail;
        if (!buffer_append_optional_bool(&buf, &device->metrics.maintenance_log_up_to_date)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"cat1_tag_up_to_date\":")) goto fail;
        if (!buffer_append_optional_bool(&buf, &device->metrics.cat1_tag_current)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"cat1_tag_date\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->cat1_tag_date)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"cat5_tag_up_to_date\":")) goto fail;
        if (!buffer_append_optional_bool(&buf, &device->metrics.cat5_tag_current)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"cat5_tag_date\":")) goto fail;
        if (!buffer_append_json_string(&buf, device->cat5_tag_date)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"code_data_year\":")) goto fail;
        if (!buffer_append_optional_int(&buf, &device->metrics.code_data_year)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"code_data_plate_present\":")) goto fail;
        if (!buffer_append_optional_bool(&buf, &device->metrics.code_data_plate_present)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"door_opening_width\":")) goto fail;
        if (!buffer_append_optional_double(&buf, &device->metrics.door_opening_width)) goto fail;
        if (!buffer_append_char(&buf, ',')) goto fail;

        if (!buffer_append_cstr(&buf, "\"is_first_car\":")) goto fail;
        if (!buffer_append_optional_bool(&buf, &device->metrics.is_first_car)) goto fail;

        if (!buffer_append_char(&buf, '}')) goto fail;
    }
    if (!buffer_append_char(&buf, '}')) goto fail; // end devices

    if (!buffer_append_char(&buf, '}')) goto fail; // end outer object

    char *json = buf.data;
    buf.data = NULL;
    buffer_free(&buf);
    return json;

fail:
    buffer_free(&buf);
    return NULL;
}

static void report_device_metrics_init(ReportDeviceMetrics *metrics) {
    optional_int_clear(&metrics->controller_install_year);
    optional_int_clear(&metrics->controller_age);
    optional_int_clear(&metrics->capacity);
    optional_int_clear(&metrics->car_speed);
    optional_int_clear(&metrics->number_of_stops);
    optional_int_clear(&metrics->number_of_openings);
    optional_int_clear(&metrics->code_data_year);
    optional_int_clear(&metrics->ride_quality);
    optional_double_clear(&metrics->door_opening_width);
    optional_bool_clear(&metrics->dlm_compliant);
    optional_bool_clear(&metrics->maintenance_log_up_to_date);
    optional_bool_clear(&metrics->cat1_tag_current);
    optional_bool_clear(&metrics->cat5_tag_current);
    optional_bool_clear(&metrics->code_data_plate_present);
    optional_bool_clear(&metrics->is_first_car);
}

static void report_device_init(ReportDevice *device) {
    if (!device) return;
    device->audit_uuid = NULL;
    device->device_id = NULL;
    device->submission_id = NULL;
    device->device_type = NULL;
    device->bank_name = NULL;
    device->city_id = NULL;
    device->general_notes = NULL;
    device->controller_manufacturer = NULL;
    device->controller_model = NULL;
    device->controller_type = NULL;
    device->controller_power_system = NULL;
    device->machine_manufacturer = NULL;
    device->machine_type = NULL;
    device->roping = NULL;
    device->door_operation = NULL;
    device->door_operation_type = NULL;
    device->cat1_tag_date = NULL;
    device->cat5_tag_date = NULL;
    device->submitted_on_iso = NULL;
    string_array_init(&device->floors_served);
    string_array_init(&device->cars_in_bank);
    string_array_init(&device->total_floor_stop_names);
    report_device_metrics_init(&device->metrics);
    report_deficiency_list_init(&device->deficiencies);
    device->docstring = REPORT_DEVICE_DOCSTRING;
    device->deficiencies_docstring = REPORT_DEFICIENCIES_DOCSTRING;
}

static void report_device_clear(ReportDevice *device) {
    if (!device) return;
    free(device->audit_uuid);
    free(device->device_id);
    free(device->submission_id);
    free(device->device_type);
    free(device->bank_name);
    free(device->city_id);
    free(device->general_notes);
    free(device->controller_manufacturer);
    free(device->controller_model);
    free(device->controller_type);
    free(device->controller_power_system);
    free(device->machine_manufacturer);
    free(device->machine_type);
    free(device->roping);
    free(device->door_operation);
    free(device->door_operation_type);
    free(device->cat1_tag_date);
    free(device->cat5_tag_date);
    free(device->submitted_on_iso);
    string_array_clear(&device->floors_served);
    string_array_clear(&device->cars_in_bank);
    string_array_clear(&device->total_floor_stop_names);
    report_deficiency_list_clear(&device->deficiencies);
    report_device_metrics_init(&device->metrics);
    device->docstring = REPORT_DEVICE_DOCSTRING;
    device->deficiencies_docstring = REPORT_DEFICIENCIES_DOCSTRING;
}

static void report_device_list_init(ReportDeviceList *list) {
    list->items = NULL;
    list->count = 0;
    list->capacity = 0;
}

static void report_device_list_clear(ReportDeviceList *list) {
    if (!list) return;
    for (size_t i = 0; i < list->count; ++i) {
        report_device_clear(&list->items[i]);
    }
    free(list->items);
    list->items = NULL;
    list->count = 0;
    list->capacity = 0;
}

static int report_device_list_append_move(ReportDeviceList *list, ReportDevice *device) {
    if (!list || !device) return 0;
    if (list->count == list->capacity) {
        size_t new_cap = list->capacity == 0 ? 4 : list->capacity * 2;
        ReportDevice *tmp = realloc(list->items, new_cap * sizeof(ReportDevice));
        if (!tmp) {
            return 0;
        }
        list->items = tmp;
        list->capacity = new_cap;
    }
    list->items[list->count] = *device;
    memset(device, 0, sizeof(*device));
    list->count++;
    return 1;
}

static void report_summary_init(ReportSummary *summary) {
    if (!summary) return;
    summary->docstring = REPORT_SUMMARY_DOCSTRING;
    summary->building_address = NULL;
    summary->building_owner = NULL;
    summary->elevator_contractor = NULL;
    summary->city_id = NULL;
    summary->audit_range.start = NULL;
    summary->audit_range.end = NULL;
    summary->total_devices = 0;
    summary->elevator_count = 0;
    summary->escalator_count = 0;
    summary->audit_count = 0;
    summary->total_deficiencies = 0;
    summary->average_deficiencies_per_device = 0.0;
    key_count_list_init(&summary->deficiencies_by_code);
}

static void report_summary_clear(ReportSummary *summary) {
    if (!summary) return;
    free(summary->building_address);
    free(summary->building_owner);
    free(summary->elevator_contractor);
    free(summary->city_id);
    free(summary->audit_range.start);
    free(summary->audit_range.end);
    key_count_list_clear(&summary->deficiencies_by_code);
    summary->total_devices = 0;
    summary->elevator_count = 0;
    summary->escalator_count = 0;
    summary->audit_count = 0;
    summary->total_deficiencies = 0;
    summary->average_deficiencies_per_device = 0.0;
}

static void report_data_init(ReportData *data) {
    if (!data) return;
    report_summary_init(&data->summary);
    report_device_list_init(&data->devices);
    device_codes_list_init(&data->deficiency_codes_by_device);
}

static void report_data_clear(ReportData *data) {
    if (!data) return;
    report_device_list_clear(&data->devices);
    report_summary_clear(&data->summary);
    device_codes_list_clear(&data->deficiency_codes_by_device);
}

static void deficiency_list_init(DeficiencyList *list) {
    list->items = NULL;
    list->count = 0;
    list->capacity = 0;
}

static void deficiency_free(Deficiency *d) {
    if (!d) return;
    free(d->violation_device_id);
    free(d->equipment_code);
    free(d->condition_code);
    free(d->remedy_code);
    free(d->overlay_code);
    free(d->violation_equipment);
    free(d->violation_condition);
    free(d->violation_remedy);
    free(d->violation_note);
}
static void deficiency_list_clear(DeficiencyList *list) {
    if (!list) {
        return;
    }
    for (size_t i = 0; i < list->count; ++i) {
        deficiency_free(&list->items[i]);
    }
    free(list->items);
    list->items = NULL;
    list->count = 0;
    list->capacity = 0;
}

static int deficiency_list_append(DeficiencyList *list, const Deficiency *value) {
    if (list->count == list->capacity) {
        size_t new_cap = list->capacity == 0 ? 4 : list->capacity * 2;
        Deficiency *tmp = realloc(list->items, new_cap * sizeof(Deficiency));
        if (!tmp) {
            return 0;
        }
        list->items = tmp;
        list->capacity = new_cap;
    }
    list->items[list->count] = *value;
    list->count++;
    return 1;
}

static void photo_collection_init(PhotoCollection *collection) {
    collection->items = NULL;
    collection->count = 0;
    collection->capacity = 0;
}

static void photo_file_clear(PhotoFile *file) {
    if (!file) return;
    free(file->filename);
    free(file->content_type);
    free(file->data);
    file->filename = NULL;
    file->content_type = NULL;
    file->data = NULL;
    file->size = 0;
}

static void photo_collection_clear(PhotoCollection *collection) {
    if (!collection) return;
    for (size_t i = 0; i < collection->count; ++i) {
        photo_file_clear(&collection->items[i]);
    }
    free(collection->items);
    collection->items = NULL;
    collection->count = 0;
    collection->capacity = 0;
}

static int photo_collection_append(PhotoCollection *collection, const char *filename, const char *content_type, unsigned char *data, size_t size) {
    if (collection->count == collection->capacity) {
        size_t new_cap = collection->capacity == 0 ? 4 : collection->capacity * 2;
        PhotoFile *tmp = realloc(collection->items, new_cap * sizeof(PhotoFile));
        if (!tmp) {
            return 0;
        }
        collection->items = tmp;
        collection->capacity = new_cap;
    }
    PhotoFile *dest = &collection->items[collection->count];
    dest->filename = strdup(filename);
    dest->content_type = strdup(content_type ? content_type : "application/octet-stream");
    dest->data = data;
    dest->size = size;
    if (!dest->filename || !dest->content_type) {
        photo_file_clear(dest);
        return 0;
    }
    collection->count++;
    return 1;
}

static const PhotoFile *photo_collection_find(const PhotoCollection *collection, const char *filename) {
    if (!collection || !filename) {
        return NULL;
    }
    for (size_t i = 0; i < collection->count; ++i) {
        if (strcmp(collection->items[i].filename, filename) == 0) {
            return &collection->items[i];
        }
    }
    return NULL;
}

static void allocation_list_init(AllocationList *list) {
    list->items = NULL;
    list->count = 0;
    list->capacity = 0;
}

static int allocation_list_add(AllocationList *list, char *ptr) {
    if (!ptr) {
        return 1;
    }
    if (list->count == list->capacity) {
        size_t new_cap = list->capacity == 0 ? 8 : list->capacity * 2;
        char **tmp = realloc(list->items, new_cap * sizeof(char *));
        if (!tmp) {
            return 0;
        }
        list->items = tmp;
        list->capacity = new_cap;
    }
    list->items[list->count++] = ptr;
    return 1;
}

static void allocation_list_clear(AllocationList *list) {
    if (!list) return;
    for (size_t i = 0; i < list->count; ++i) {
        free(list->items[i]);
    }
    free(list->items);
    list->items = NULL;
    list->count = 0;
    list->capacity = 0;
}

static void audit_record_init(AuditRecord *record) {
    memset(record, 0, sizeof(*record));
    string_array_init(&record->cars_in_bank);
    string_array_init(&record->total_floor_stop_names);
    string_array_init(&record->floors_served);
    optional_int_clear(&record->form_version);
    optional_long_clear(&record->form_id);
    optional_long_clear(&record->account_id);
    optional_long_clear(&record->user_id);
    optional_int_clear(&record->rating_overall);
    optional_bool_clear(&record->is_first_car);
    optional_int_clear(&record->controller_install_year);
    optional_int_clear(&record->car_speed);
    optional_bool_clear(&record->dlm_compliant);
    optional_bool_clear(&record->maintenance_log_up_to_date);
    optional_bool_clear(&record->code_data_plate_present);
    optional_int_clear(&record->code_data_year);
    optional_bool_clear(&record->cat1_tag_current);
    optional_bool_clear(&record->cat5_tag_current);
    optional_bool_clear(&record->brake_tag_current);
    optional_int_clear(&record->number_of_ropes);
    optional_int_clear(&record->rope_condition_score);
    optional_bool_clear(&record->motor_data_plate_present);
    optional_bool_clear(&record->rope_gripper_present);
    optional_bool_clear(&record->counterweight_governor);
    optional_bool_clear(&record->tank_heater_present);
    optional_bool_clear(&record->oil_cooler_present);
    optional_int_clear(&record->capacity);
    optional_int_clear(&record->number_of_openings);
    optional_int_clear(&record->number_of_stops);
    optional_bool_clear(&record->has_hoistway_access_keyswitches);
    optional_bool_clear(&record->sump_pump_present);
    optional_bool_clear(&record->scavenger_pump_present);
    optional_double_clear(&record->door_opening_width);
    optional_int_clear(&record->expected_stop_count);
    optional_long_clear(&record->mobile_memory_mb);
}

static void audit_record_free(AuditRecord *record) {
    if (!record) return;
    free(record->audit_uuid);
    free(record->form_name);
    free(record->submitted_on);
    free(record->submitted_by);
    free(record->updated_at);
    free(record->user_name);
    free(record->submit_guid);
    free(record->workflow_stage);
    free(record->workflow_user);
    free(record->building_address);
    free(record->building_owner);
    free(record->elevator_contractor);
    free(record->city_id);
    free(record->building_id);
    free(record->device_type);
    free(record->building_information);
    free(record->bank_name);
    free(record->machine_room_location);
    free(record->machine_room_location_other);
    free(record->controller_manufacturer);
    free(record->controller_model);
    free(record->controller_type);
    free(record->controller_power_system);
    free(record->last_maintenance_log_date);
    free(record->cat1_tag_date);
    free(record->cat5_tag_date);
    free(record->brake_tag_date);
    free(record->machine_manufacturer);
    free(record->machine_type);
    free(record->roping);
    free(record->motor_type);
    free(record->brake_type);
    free(record->single_or_dual_core_brake);
    free(record->governor_manufacturer);
    free(record->governor_type);
    free(record->pump_motor_manufacturer);
    free(record->oil_condition);
    free(record->oil_level);
    free(record->valve_manufacturer);
    free(record->door_operation);
    free(record->door_operation_type);
    free(record->pi_type);
    free(record->rail_type);
    free(record->guide_type);
    free(record->car_door_equipment_manufacturer);
    free(record->car_door_lock_manufacturer);
    free(record->car_door_operator_manufacturer);
    free(record->car_door_operator_model);
    free(record->restrictor_type);
    free(record->hallway_pi_type);
    free(record->hatch_door_unlocking_type);
    free(record->hatch_door_equipment_manufacturer);
    free(record->hatch_door_lock_manufacturer);
    free(record->pit_access);
    free(record->safety_type);
    free(record->buffer_type);
    free(record->compensation_type);
    free(record->jack_piston_type);
    free(record->general_notes);
    free(record->mobile_device);
    free(record->mobile_app_name);
    free(record->mobile_app_version);
    free(record->mobile_app_type);
    free(record->mobile_sdk_release);
    string_array_clear(&record->cars_in_bank);
    string_array_clear(&record->total_floor_stop_names);
    string_array_clear(&record->floors_served);
}



static int assign_string(char **dest, const char *value) {
    if (!value) {
        return 1;
    }
    if (value[0] == '\0') {
        return 1;
    }
    char *copy = strdup(value);
    if (!copy) {
        return 0;
    }
    free(*dest);
    *dest = copy;
    return 1;
}

static OptionalBool parse_optional_bool(const char *text) {
    OptionalBool result;
    optional_bool_clear(&result);
    if (!text) {
        return result;
    }
    char *trimmed = trim_copy(text);
    if (!trimmed) {
        return result;
    }
    if (trimmed[0] == '\0') {
        free(trimmed);
        return result;
    }
    if (strcmp(trimmed, "1") == 0 ||
        strcasecmp(trimmed, "true") == 0 ||
        strcasecmp(trimmed, "yes") == 0 ||
        strcasecmp(trimmed, "y") == 0 ||
        strcasecmp(trimmed, "t") == 0 ||
        strcasecmp(trimmed, "on") == 0) {
        result.has_value = true;
        result.value = true;
    } else if (strcmp(trimmed, "0") == 0 ||
               strcasecmp(trimmed, "false") == 0 ||
               strcasecmp(trimmed, "no") == 0 ||
               strcasecmp(trimmed, "n") == 0 ||
               strcasecmp(trimmed, "f") == 0 ||
               strcasecmp(trimmed, "off") == 0) {
        result.has_value = true;
        result.value = false;
    }
    free(trimmed);
    return result;
}

static OptionalInt parse_optional_int(const char *text) {
    OptionalInt result;
    optional_int_clear(&result);
    if (!text) return result;
    char *trimmed = trim_copy(text);
    if (!trimmed) return result;
    if (trimmed[0] == '\0') {
        free(trimmed);
        return result;
    }
    char *endptr = NULL;
    long value = strtol(trimmed, &endptr, 10);
    if (endptr && *endptr == '\0') {
        result.has_value = true;
        result.value = (int)value;
    }
    free(trimmed);
    return result;
}

static OptionalLong parse_optional_long(const char *text) {
    OptionalLong result;
    optional_long_clear(&result);
    if (!text) return result;
    char *trimmed = trim_copy(text);
    if (!trimmed) return result;
    if (trimmed[0] == '\0') {
        free(trimmed);
        return result;
    }
    char *endptr = NULL;
    long value = strtol(trimmed, &endptr, 10);
    if (endptr && *endptr == '\0') {
        result.has_value = true;
        result.value = value;
    }
    free(trimmed);
    return result;
}

static OptionalDouble parse_optional_double(const char *text) {
    OptionalDouble result;
    optional_double_clear(&result);
    if (!text) return result;
    char *trimmed = trim_copy(text);
    if (!trimmed) return result;
    if (trimmed[0] == '\0') {
        free(trimmed);
        return result;
    }
    char *endptr = NULL;
    double value = strtod(trimmed, &endptr);
    if (endptr && *endptr == '\0') {
        result.has_value = true;
        result.value = value;
    }
    free(trimmed);
    return result;
}

static char *convert_submitted_on_to_iso(const char *input) {
    if (!input) return NULL;
    struct tm tm_value;
    memset(&tm_value, 0, sizeof(tm_value));
    char *trimmed = trim_copy(input);
    if (!trimmed) return NULL;
    if (trimmed[0] == '\0') {
        free(trimmed);
        return NULL;
    }
    const char *formats[] = {"%m-%d-%Y %H:%M", "%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S"};
    bool parsed = false;
    for (size_t i = 0; i < sizeof(formats)/sizeof(formats[0]); ++i) {
        char *res = strptime(trimmed, formats[i], &tm_value);
        if (res && *res == '\0') {
            parsed = true;
            break;
        }
    }
    char buffer[32];
    if (parsed) {
        tm_value.tm_isdst = -1;
        time_t ts = mktime(&tm_value);
        struct tm *utc = gmtime(&ts);
        if (utc) {
            strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S%z", utc);
        } else {
            strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tm_value);
        }
    } else {
        snprintf(buffer, sizeof(buffer), "%s", trimmed);
    }
    free(trimmed);
    return strdup(buffer);
}

static int string_array_expand_range(StringArray *array, const char *start_token, const char *end_token) {
    OptionalInt start = parse_optional_int(start_token);
    OptionalInt end = parse_optional_int(end_token);
    if (!start.has_value || !end.has_value || end.value < start.value) {
        return 0;
    }
    for (int value = start.value; value <= end.value; ++value) {
        char buf[32];
        snprintf(buf, sizeof(buf), "%d", value);
        if (!string_array_append_copy(array, buf)) {
            return -1;
        }
    }
    return 1;
}

static int parse_delimited_floor_list(const char *text, StringArray *array) {
    if (!text) {
        return 1;
    }
    char *copy = strdup(text);
    if (!copy) {
        return 0;
    }
    char *token = strtok(copy, ",");
    while (token) {
        char *trimmed = trim_copy(token);
        if (!trimmed) {
            free(copy);
            return 0;
        }
        if (strchr(trimmed, '-') != NULL) {
            char *dash = strchr(trimmed, '-');
            *dash = '\0';
            const char *start = trimmed;
            const char *end = dash + 1;
            int res = string_array_expand_range(array, start, end);
            if (res < 0) {
                free(trimmed);
                free(copy);
                return 0;
            }
            if (res == 0) {
                if (!string_array_append_copy(array, trimmed)) {
                    free(trimmed);
                    free(copy);
                    return 0;
                }
            }
        } else {
            if (!string_array_append_copy(array, trimmed)) {
                free(trimmed);
                free(copy);
                return 0;
            }
        }
        free(trimmed);
        token = strtok(NULL, ",");
    }
    free(copy);
    return 1;
}

static int parse_simple_list(const char *text, StringArray *array) {
    if (!text) {
        return 1;
    }
    char *copy = strdup(text);
    if (!copy) return 0;
    char *token = strtok(copy, ",");
    while (token) {
        char *trimmed = trim_copy(token);
        if (!trimmed) {
            free(copy);
            return 0;
        }
        if (!string_array_append_copy(array, trimmed)) {
            free(trimmed);
            free(copy);
            return 0;
        }
        free(trimmed);
        token = strtok(NULL, ",");
    }
    free(copy);
    return 1;
}

static char *pg_array_from_string_array(const StringArray *array) {
    if (!array || array->count == 0) {
        char *empty = strdup("{}");
        return empty;
    }
    size_t capacity = 2; // {}
    for (size_t i = 0; i < array->count; ++i) {
        capacity += 3; // quotes and maybe comma
        const char *val = array->values[i];
        for (const char *c = val; *c; ++c) {
            if (*c == '"' || *c == '\\') {
                capacity += 2;
            } else {
                capacity += 1;
            }
        }
    }
    char *buffer = malloc(capacity + 1);
    if (!buffer) {
        return NULL;
    }
    size_t pos = 0;
    buffer[pos++] = '{';
    for (size_t i = 0; i < array->count; ++i) {
        if (i > 0) {
            buffer[pos++] = ',';
        }
        buffer[pos++] = '"';
        const char *val = array->values[i];
        for (const char *c = val; *c; ++c) {
            if (*c == '"' || *c == '\\') {
                buffer[pos++] = '\\';
            }
            buffer[pos++] = *c;
        }
        buffer[pos++] = '"';
    }
    buffer[pos++] = '}';
    buffer[pos] = '\0';
    return buffer;
}

static int read_file_to_string(const char *path, char **out_text) {
    *out_text = NULL;
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        log_error("Failed to open %s: %s", path, strerror(errno));
        return 0;
    }
    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return 0;
    }
    long size = ftell(fp);
    if (size < 0) {
        fclose(fp);
        return 0;
    }
    if (fseek(fp, 0, SEEK_SET) != 0) {
        fclose(fp);
        return 0;
    }
    char *buffer = malloc((size_t)size + 1);
    if (!buffer) {
        fclose(fp);
        return 0;
    }
    size_t read = fread(buffer, 1, (size_t)size, fp);
    fclose(fp);
    if (read != (size_t)size) {
        free(buffer);
        return 0;
    }
    buffer[size] = '\0';
    *out_text = buffer;
    return 1;
}

static int read_file_to_bytes(const char *path, unsigned char **out_data, size_t *out_size) {
    *out_data = NULL;
    *out_size = 0;
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        log_error("Failed to open %s: %s", path, strerror(errno));
        return 0;
    }
    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return 0;
    }
    long size = ftell(fp);
    if (size < 0) {
        fclose(fp);
        return 0;
    }
    if (fseek(fp, 0, SEEK_SET) != 0) {
        fclose(fp);
        return 0;
    }
    unsigned char *buffer = malloc((size_t)size);
    if (!buffer) {
        fclose(fp);
        return 0;
    }
    size_t read = fread(buffer, 1, (size_t)size, fp);
    fclose(fp);
    if (read != (size_t)size) {
        free(buffer);
        return 0;
    }
    *out_data = buffer;
    *out_size = (size_t)size;
    return 1;
}

static int write_all(int fd, const void *data, size_t len) {
    const unsigned char *ptr = (const unsigned char *)data;
    size_t written = 0;
    while (written < len) {
        ssize_t w = write(fd, ptr + written, len - written);
        if (w < 0) {
            if (errno == EINTR) continue;
            return 0;
        }
        written += (size_t)w;
    }
    return 1;
}

static const char *get_basename(const char *path) {
    const char *slash = strrchr(path, '/');
    if (slash) {
        return slash + 1;
    }
    slash = strrchr(path, '\\');
    if (slash) {
        return slash + 1;
    }
    return path;
}

static const char *guess_content_type(const char *filename) {
    const char *dot = strrchr(filename, '.');
    if (!dot) {
        return "application/octet-stream";
    }
    if (strcasecmp(dot, ".jpg") == 0 || strcasecmp(dot, ".jpeg") == 0) {
        return "image/jpeg";
    }
    if (strcasecmp(dot, ".png") == 0) {
        return "image/png";
    }
    return "application/octet-stream";
}

static int remove_directory_recursive(const char *path) {
    DIR *dir = opendir(path);
    if (!dir) {
        if (errno == ENOTDIR) {
            return unlink(path);
        }
        return -1;
    }
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        char child_path[PATH_MAX];
        snprintf(child_path, sizeof(child_path), "%s/%s", path, entry->d_name);
        struct stat st;
        if (lstat(child_path, &st) == 0) {
            if (S_ISDIR(st.st_mode)) {
                if (remove_directory_recursive(child_path) != 0) {
                    closedir(dir);
                    return -1;
                }
            } else {
                if (unlink(child_path) != 0) {
                    closedir(dir);
                    return -1;
                }
            }
        }
    }
    closedir(dir);
    if (rmdir(path) != 0) {
        return -1;
    }
    return 0;
}
static int collect_files_recursive(const char *root, char **csv_path, char **json_path, PhotoCollection *photos) {
    DIR *dir = opendir(root);
    if (!dir) {
        log_error("Failed to open directory %s: %s", root, strerror(errno));
        return 0;
    }
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        char full_path[PATH_MAX];
        snprintf(full_path, sizeof(full_path), "%s/%s", root, entry->d_name);
        struct stat st;
        if (stat(full_path, &st) != 0) {
            closedir(dir);
            return 0;
        }
        if (S_ISDIR(st.st_mode)) {
            if (!collect_files_recursive(full_path, csv_path, json_path, photos)) {
                closedir(dir);
                return 0;
            }
            continue;
        }
        const char *basename = get_basename(full_path);
        const char *ext = strrchr(basename, '.');
        if (ext && strcasecmp(ext, ".csv") == 0) {
            if (!*csv_path) {
                *csv_path = strdup(full_path);
            }
            continue;
        }
        if (ext && strcasecmp(ext, ".json") == 0) {
            if (!*json_path) {
                *json_path = strdup(full_path);
            }
            continue;
        }
        if (ext && (strcasecmp(ext, ".jpg") == 0 || strcasecmp(ext, ".jpeg") == 0 || strcasecmp(ext, ".png") == 0)) {
            unsigned char *data = NULL;
            size_t size = 0;
            if (!read_file_to_bytes(full_path, &data, &size)) {
                closedir(dir);
                return 0;
            }
            if (!photo_collection_append(photos, basename, guess_content_type(basename), data, size)) {
                free(data);
                closedir(dir);
                return 0;
            }
            continue;
        }
    }
    closedir(dir);
    return 1;
}

static int collect_files(const char *root_dir, char **csv_path, char **json_path, PhotoCollection *photos) {
    *csv_path = NULL;
    *json_path = NULL;
    photo_collection_init(photos);
    if (!collect_files_recursive(root_dir, csv_path, json_path, photos)) {
        free(*csv_path);
        free(*json_path);
        photo_collection_clear(photos);
        *csv_path = NULL;
        *json_path = NULL;
        return 0;
    }
    return 1;
}

static char *join_json_string_array(const JsonValue *value) {
    if (!value || value->type != JSON_ARRAY) {
        return NULL;
    }
    size_t total = 0;
    for (size_t i = 0; i < value->value.array.count; ++i) {
        const JsonValue *item = value->value.array.items[i];
        const char *str = json_as_string(item);
        if (!str) continue;
        total += strlen(str) + 2; // for delimiter
    }
    if (total == 0) {
        return NULL;
    }
    char *buffer = malloc(total + 1);
    if (!buffer) {
        return NULL;
    }
    buffer[0] = '\0';
    bool first = true;
    for (size_t i = 0; i < value->value.array.count; ++i) {
        const JsonValue *item = value->value.array.items[i];
        const char *str = json_as_string(item);
        if (!str) continue;
        if (!first) {
            strcat(buffer, "; ");
        }
        first = false;
        strcat(buffer, str);
    }
    return buffer;
}

static int parse_photo_names(const JsonValue *root, StringArray *photos) {
    string_array_init(photos);
    JsonValue *photo_array = json_object_get(root, "multiphoto_picker_8");
    if (!photo_array || photo_array->type != JSON_ARRAY) {
        return 1;
    }
    for (size_t i = 0; i < photo_array->value.array.count; ++i) {
        JsonValue *item = photo_array->value.array.items[i];
        if (!item || item->type != JSON_OBJECT) {
            continue;
        }
        JsonValue *photo_value = json_object_get(item, "photo");
        const char *photo_name = json_as_string(photo_value);
        if (photo_name && !string_array_append_copy(photos, photo_name)) {
            string_array_clear(photos);
            return 0;
        }
    }
    return 1;
}

static int parse_deficiencies(const JsonValue *root, DeficiencyList *list) {
    deficiency_list_init(list);
    JsonValue *defs = json_object_get(root, "DEFICIENCIES");
    if (!defs || defs->type != JSON_ARRAY) {
        return 1;
    }
    for (size_t i = 0; i < defs->value.array.count; ++i) {
        JsonValue *def_obj = defs->value.array.items[i];
        if (!def_obj || def_obj->type != JSON_OBJECT) {
            continue;
        }
        int section_counter = 0;
        JsonValue *section_val = json_object_get(def_obj, "sectionCounter");
        json_as_int(section_val, &section_counter);

        JsonValue *fields = json_object_get(def_obj, "fields");
        if (!fields || fields->type != JSON_OBJECT) {
            continue;
        }
        const char *device_id_str = NULL;
        JsonValue *device_id_val = json_object_get(fields, "VIOLATION_DEVICE_ID");
        if (device_id_val) {
            device_id_str = json_as_string(device_id_val);
        }

        JsonValue *subforms = json_object_get(fields, "VIOLATION_SUBFORM");
        if (!subforms || subforms->type != JSON_ARRAY || subforms->value.array.count == 0) {
            Deficiency def;
            memset(&def, 0, sizeof(def));
            def.section_counter = section_counter;
            if (device_id_str) def.violation_device_id = strdup(device_id_str);
            if (!def.violation_device_id && device_id_str) {
                return 0;
            }
            if (!deficiency_list_append(list, &def)) {
                deficiency_free(&def);
                return 0;
            }
            continue;
        }

        for (size_t j = 0; j < subforms->value.array.count; ++j) {
            JsonValue *sf = subforms->value.array.items[j];
            if (!sf || sf->type != JSON_OBJECT) continue;
            Deficiency def;
            memset(&def, 0, sizeof(def));
            def.section_counter = section_counter;
            if (device_id_str) {
                def.violation_device_id = strdup(device_id_str);
                if (!def.violation_device_id) {
                    return 0;
                }
            }
            JsonValue *equip = json_object_get(sf, "EQUIPMENT_CODE");
            JsonValue *cond = json_object_get(sf, "CONDITION_CODE");
            JsonValue *remedy = json_object_get(sf, "REMEDY_CODE");
            JsonValue *overlay = json_object_get(sf, "OVERLAY_CODE_CALC");
            JsonValue *note = json_object_get(sf, "VIOLATION_NOTE");
            JsonValue *equip_list = json_object_get(sf, "VIOLATION_EQUIPMENT");
            JsonValue *cond_list = json_object_get(sf, "VIOLATION_CONDITION");
            JsonValue *remedy_list = json_object_get(sf, "VIOLATION_REMEDY");

            const char *equip_str = json_as_string(equip);
            const char *cond_str = json_as_string(cond);
            const char *remedy_str = json_as_string(remedy);
            const char *overlay_str = json_as_string(overlay);
            const char *note_str = json_as_string(note);

            if (equip_str) def.equipment_code = strdup(equip_str);
            if (cond_str) def.condition_code = strdup(cond_str);
            if (remedy_str) def.remedy_code = strdup(remedy_str);
            if (overlay_str) def.overlay_code = strdup(overlay_str);
            if (note_str) def.violation_note = strdup(note_str);

            def.violation_equipment = join_json_string_array(equip_list);
            def.violation_condition = join_json_string_array(cond_list);
            def.violation_remedy = join_json_string_array(remedy_list);

            if (!deficiency_list_append(list, &def)) {
                deficiency_free(&def);
                return 0;
            }
        }
    }
    return 1;
}
static int populate_audit_record(const CsvFile *csv, const CsvRow *row, const JsonValue *json_root, AuditRecord *record, char **error_out) {
    audit_record_init(record);
    const char *submission_id = csv_row_get(csv, row, "Submission Id");
    if (!submission_id || submission_id[0] == '\0') {
        if (error_out) {
            *error_out = strdup("Submission Id is missing in CSV");
        }
        return 0;
    }
    record->audit_uuid = strdup(submission_id);
    if (!record->audit_uuid) {
        if (error_out) *error_out = strdup("Out of memory allocating audit UUID");
        return 0;
    }

    record->form_id = parse_optional_long(csv_row_get(csv, row, "FormId"));
    record->form_version = parse_optional_int(csv_row_get(csv, row, "Form Version"));
    if (!assign_string(&record->form_name, csv_row_get(csv, row, "Form Name"))) goto oom;
    record->submitted_on = convert_submitted_on_to_iso(csv_row_get(csv, row, "Submitted On"));
    if (csv_row_get(csv, row, "Submitted On") && !record->submitted_on) goto oom;
    if (!assign_string(&record->submitted_by, csv_row_get(csv, row, "Submitted By"))) goto oom;

    if (!assign_string(&record->building_address, csv_row_get(csv, row, "Building Address"))) goto oom;
    if (!assign_string(&record->building_owner, csv_row_get(csv, row, "Building Owner"))) goto oom;
    if (!assign_string(&record->elevator_contractor, csv_row_get(csv, row, "Elevator Contractor"))) goto oom;
    if (!assign_string(&record->city_id, csv_row_get(csv, row, "City ID"))) goto oom;
    if (!assign_string(&record->building_id, csv_row_get(csv, row, "Building ID"))) goto oom;
    if (!assign_string(&record->device_type, csv_row_get(csv, row, "Device Type"))) goto oom;

    record->is_first_car = parse_optional_bool(csv_row_get(csv, row, "Is This the First or Only Car in the Bank?"));
    if (!assign_string(&record->building_information, csv_row_get(csv, row, "Building Information"))) goto oom;
    if (!assign_string(&record->bank_name, csv_row_get(csv, row, "Bank Name"))) goto oom;

    if (!parse_simple_list(csv_row_get(csv, row, "Cars In Bank"), &record->cars_in_bank)) goto oom;
    if (!parse_delimited_floor_list(csv_row_get(csv, row, "Total Building Floor Stop Names"), &record->total_floor_stop_names)) goto oom;
    if (!parse_delimited_floor_list(csv_row_get(csv, row, "Floors Served"), &record->floors_served)) goto oom;

    if (!assign_string(&record->machine_room_location, csv_row_get(csv, row, "Machine Room Location"))) goto oom;
    if (!assign_string(&record->machine_room_location_other, csv_row_get(csv, row, "Explain Other Machine Room Location"))) goto oom;
    if (!assign_string(&record->controller_manufacturer, csv_row_get(csv, row, "Controller Manufacturer"))) goto oom;
    if (!assign_string(&record->controller_model, csv_row_get(csv, row, "Controller Model"))) goto oom;
    record->controller_install_year = parse_optional_int(csv_row_get(csv, row, "Controller Installation Year"));
    if (!assign_string(&record->controller_type, csv_row_get(csv, row, "Controller Type"))) goto oom;
    if (!assign_string(&record->controller_power_system, csv_row_get(csv, row, "Controller Power System"))) goto oom;
    record->car_speed = parse_optional_int(csv_row_get(csv, row, "Car Speed"));

    record->dlm_compliant = parse_optional_bool(csv_row_get(csv, row, "DLM Compliant?"));
    record->maintenance_log_up_to_date = parse_optional_bool(csv_row_get(csv, row, "Maintenance Log Up To Date?"));
    if (!assign_string(&record->last_maintenance_log_date, csv_row_get(csv, row, "Last Maintenance Log Date"))) goto oom;
    record->code_data_plate_present = parse_optional_bool(csv_row_get(csv, row, "Code Data Plate On Controller?"));
    record->code_data_year = parse_optional_int(csv_row_get(csv, row, "Code Data Year"));
    record->cat1_tag_current = parse_optional_bool(csv_row_get(csv, row, "Cat1 Tag Up To Date?"));
    if (!assign_string(&record->cat1_tag_date, csv_row_get(csv, row, "Cat1 Tag Date"))) goto oom;
    record->cat5_tag_current = parse_optional_bool(csv_row_get(csv, row, "Cat5 Tag Up To Date?"));
    if (!assign_string(&record->cat5_tag_date, csv_row_get(csv, row, "Cat5 Tag Date"))) goto oom;
    record->brake_tag_current = parse_optional_bool(csv_row_get(csv, row, "Brake Maintenance Tag Up To Date?"));
    if (!assign_string(&record->brake_tag_date, csv_row_get(csv, row, "Brake Maintenance Tag Date"))) goto oom;

    if (!assign_string(&record->machine_manufacturer, csv_row_get(csv, row, "Machine Manufacturer"))) goto oom;
    if (!assign_string(&record->machine_type, csv_row_get(csv, row, "Machine Type"))) goto oom;
    record->number_of_ropes = parse_optional_int(csv_row_get(csv, row, "Number of Ropes"));
    if (!assign_string(&record->roping, csv_row_get(csv, row, "Roping"))) goto oom;
    record->rope_condition_score = parse_optional_int(csv_row_get(csv, row, "Rope Condition"));
    record->motor_data_plate_present = parse_optional_bool(csv_row_get(csv, row, "Motor Data Plate Present?"));
    if (!assign_string(&record->motor_type, csv_row_get(csv, row, "Motor Type"))) goto oom;
    if (!assign_string(&record->brake_type, csv_row_get(csv, row, "Brake Type"))) goto oom;
    if (!assign_string(&record->single_or_dual_core_brake, csv_row_get(csv, row, "Single or Dual Core Brake"))) goto oom;
    record->rope_gripper_present = parse_optional_bool(csv_row_get(csv, row, "Rope Gripper Present?"));
    if (!assign_string(&record->governor_manufacturer, csv_row_get(csv, row, "Governor Manufacturer"))) goto oom;
    if (!assign_string(&record->governor_type, csv_row_get(csv, row, "Governor Type"))) goto oom;
    record->counterweight_governor = parse_optional_bool(csv_row_get(csv, row, "Counterweight Governor?"));
    if (!assign_string(&record->pump_motor_manufacturer, csv_row_get(csv, row, "Pump Motor Manufacturer"))) goto oom;
    if (!assign_string(&record->oil_condition, csv_row_get(csv, row, "Oil Condition"))) goto oom;
    if (!assign_string(&record->oil_level, csv_row_get(csv, row, "Oil Level"))) goto oom;
    if (!assign_string(&record->valve_manufacturer, csv_row_get(csv, row, "Valve Manufacturer"))) goto oom;
    record->tank_heater_present = parse_optional_bool(csv_row_get(csv, row, "Tank Heater Present?"));
    record->oil_cooler_present = parse_optional_bool(csv_row_get(csv, row, "Oil Cooler Present?"));
    record->capacity = parse_optional_int(csv_row_get(csv, row, "Capacity"));
    if (!assign_string(&record->door_operation, csv_row_get(csv, row, "Door Operation"))) goto oom;
    if (!assign_string(&record->door_operation_type, csv_row_get(csv, row, "Door Operation Type"))) goto oom;
    record->number_of_openings = parse_optional_int(csv_row_get(csv, row, "Number of Openings"));
    record->number_of_stops = parse_optional_int(csv_row_get(csv, row, "Number of Stops"));

    if (!assign_string(&record->pi_type, csv_row_get(csv, row, "P.I. Type"))) goto oom;
    if (!assign_string(&record->rail_type, csv_row_get(csv, row, "Rail Type"))) goto oom;
    if (!assign_string(&record->guide_type, csv_row_get(csv, row, "Guide Type"))) goto oom;
    if (!assign_string(&record->car_door_equipment_manufacturer, csv_row_get(csv, row, "Car Door Equipment Manufacturer"))) goto oom;
    if (!assign_string(&record->car_door_lock_manufacturer, csv_row_get(csv, row, "Car Door Lock Manufacturer"))) goto oom;
    if (!assign_string(&record->car_door_operator_manufacturer, csv_row_get(csv, row, "Car Door Operator Manufacturer"))) goto oom;
    if (!assign_string(&record->car_door_operator_model, csv_row_get(csv, row, "Car Door Operator Model"))) goto oom;
    if (!assign_string(&record->restrictor_type, csv_row_get(csv, row, "Restrictor Type"))) goto oom;
    record->has_hoistway_access_keyswitches = parse_optional_bool(csv_row_get(csv, row, "Car Has Hoistway Access Keyswitches?"));
    if (!assign_string(&record->hallway_pi_type, csv_row_get(csv, row, "Hallway PI Type"))) goto oom;
    if (!assign_string(&record->hatch_door_unlocking_type, csv_row_get(csv, row, "Hatch Door Unlocking Type"))) goto oom;
    if (!assign_string(&record->hatch_door_equipment_manufacturer, csv_row_get(csv, row, "Hatch Door Equipment Manufacturer"))) goto oom;
    if (!assign_string(&record->hatch_door_lock_manufacturer, csv_row_get(csv, row, "Hatch Door Lock Manufacturer"))) goto oom;
    if (!assign_string(&record->pit_access, csv_row_get(csv, row, "Pit Access"))) goto oom;
    if (!assign_string(&record->safety_type, csv_row_get(csv, row, "Safety Type"))) goto oom;
    if (!assign_string(&record->buffer_type, csv_row_get(csv, row, "Buffer Type"))) goto oom;
    record->sump_pump_present = parse_optional_bool(csv_row_get(csv, row, "Sump Pump Present?"));
    if (!assign_string(&record->compensation_type, csv_row_get(csv, row, "Compensation Type"))) goto oom;
    if (!assign_string(&record->jack_piston_type, csv_row_get(csv, row, "Jack / Piston Type"))) goto oom;
    record->scavenger_pump_present = parse_optional_bool(csv_row_get(csv, row, "Scavenger Pump Present?"));
    if (!assign_string(&record->general_notes, csv_row_get(csv, row, "General Notes"))) goto oom;

    if (json_root) {
        JsonValue *json_submission = json_object_get(json_root, "submissionId");
        const char *json_submission_str = json_as_string(json_submission);
        if (json_submission_str && strcmp(json_submission_str, record->audit_uuid) != 0) {
            log_info("Warning: submissionId mismatch between CSV and JSON (%s vs %s)", record->audit_uuid, json_submission_str);
        }
        if (!record->form_id.has_value) {
            long json_form_id = 0;
            if (json_as_long(json_object_get(json_root, "formId"), &json_form_id)) {
                record->form_id.has_value = true;
                record->form_id.value = json_form_id;
            }
        }
        if (!record->form_version.has_value) {
            int json_form_version = 0;
            if (json_as_int(json_object_get(json_root, "formVersion"), &json_form_version)) {
                record->form_version.has_value = true;
                record->form_version.value = json_form_version;
            }
        }
        if (!record->form_name) {
            if (!assign_string(&record->form_name, json_as_string(json_object_get(json_root, "formName")))) goto oom;
        }
        if (!assign_string(&record->updated_at, json_as_string(json_object_get(json_root, "updatedAt")))) goto oom;
        long account_id = 0;
        if (json_as_long(json_object_get(json_root, "accountId"), &account_id)) {
            record->account_id.has_value = true;
            record->account_id.value = account_id;
        }
        long user_id = 0;
        if (json_as_long(json_object_get(json_root, "userId"), &user_id)) {
            record->user_id.has_value = true;
            record->user_id.value = user_id;
        }
        if (!assign_string(&record->user_name, json_as_string(json_object_get(json_root, "userName")))) goto oom;
        if (!assign_string(&record->submit_guid, json_as_string(json_object_get(json_root, "submitId")))) goto oom;
        int rating = 0;
        if (json_as_int(json_object_get(json_root, "rating_1"), &rating)) {
            record->rating_overall.has_value = true;
            record->rating_overall.value = rating;
        }
        if (!assign_string(&record->workflow_stage, json_as_string(json_object_get_path(json_root, "workflowData.stage")))) goto oom;
        if (!assign_string(&record->workflow_user, json_as_string(json_object_get_path(json_root, "workflowData.stages[0].userName")))) goto oom;
        record->door_opening_width = parse_optional_double(json_as_string(json_object_get(json_root, "numeric_4")));
        if (!record->door_opening_width.has_value) {
            JsonValue *door_width_val = json_object_get(json_root, "numeric_4");
            if (door_width_val) {
                double width = json_as_double_default(door_width_val, 0.0);
                if (width != 0.0) {
                    record->door_opening_width.has_value = true;
                    record->door_opening_width.value = width;
                }
            }
        }
        int expected_stops = 0;
        if (json_as_int(json_object_get(json_root, "numeric_5"), &expected_stops)) {
            record->expected_stop_count.has_value = true;
            record->expected_stop_count.value = expected_stops;
        }
        JsonValue *device_meta = json_object_get_path(json_root, "formMetaData.deviceMetaData");
        if (device_meta && device_meta->type == JSON_OBJECT) {
            if (!assign_string(&record->mobile_device, json_as_string(json_object_get(device_meta, "device")))) goto oom;
            if (!assign_string(&record->mobile_app_name, json_as_string(json_object_get(device_meta, "appName")))) goto oom;
            if (!assign_string(&record->mobile_app_version, json_as_string(json_object_get(device_meta, "appVersion")))) goto oom;
            if (!assign_string(&record->mobile_app_type, json_as_string(json_object_get(device_meta, "appType")))) goto oom;
            if (!assign_string(&record->mobile_sdk_release, json_as_string(json_object_get(device_meta, "sdkRelease")))) goto oom;
            long mem_mb = 0;
            if (json_as_long(json_object_get(device_meta, "totalMemoryMb"), &mem_mb)) {
                record->mobile_memory_mb.has_value = true;
                record->mobile_memory_mb.value = mem_mb;
            }
        }
    }

    return 1;

oom:
    if (error_out && !*error_out) {
        *error_out = strdup("Out of memory while populating audit record");
    }
    return 0;
}
static const char *optional_int_param(const OptionalInt *value, AllocationList *pool) {
    if (!value->has_value) return NULL;
    char buf[32];
    snprintf(buf, sizeof(buf), "%d", value->value);
    char *copy = strdup(buf);
    if (!copy) return NULL;
    if (!allocation_list_add(pool, copy)) {
        free(copy);
        return NULL;
    }
    return copy;
}

static const char *optional_long_param(const OptionalLong *value, AllocationList *pool) {
    if (!value->has_value) return NULL;
    char buf[32];
    snprintf(buf, sizeof(buf), "%ld", value->value);
    char *copy = strdup(buf);
    if (!copy) return NULL;
    if (!allocation_list_add(pool, copy)) {
        free(copy);
        return NULL;
    }
    return copy;
}

static const char *optional_double_param(const OptionalDouble *value, AllocationList *pool) {
    if (!value->has_value) return NULL;
    char buf[64];
    snprintf(buf, sizeof(buf), "%f", value->value);
    char *copy = strdup(buf);
    if (!copy) return NULL;
    if (!allocation_list_add(pool, copy)) {
        free(copy);
        return NULL;
    }
    return copy;
}

static const char *optional_bool_param(const OptionalBool *value) {
    if (!value->has_value) return NULL;
    return value->value ? "true" : "false";
}

static int db_exec_simple(PGconn *conn, const char *sql, char **error_out) {
    PGresult *res = PQexec(conn, sql);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        if (error_out) {
            const char *errmsg = PQresultErrorMessage(res);
            *error_out = strdup(errmsg ? errmsg : "Database command failed");
        }
        PQclear(res);
        return 0;
    }
    PQclear(res);
    return 1;
}

static int db_begin(PGconn *conn, char **error_out) {
    return db_exec_simple(conn, "BEGIN", error_out);
}

static int db_commit(PGconn *conn, char **error_out) {
    return db_exec_simple(conn, "COMMIT", error_out);
}

static void db_rollback(PGconn *conn) {
    PGresult *res = PQexec(conn, "ROLLBACK");
    PQclear(res);
}

#define AUDIT_PARAM_COUNT 98

static int db_insert_audit(PGconn *conn, const AuditRecord *record, char **error_out) {
    const char *delete_sql = "DELETE FROM audits WHERE audit_uuid = $1";
    const char *delete_params[1] = { record->audit_uuid };
    PGresult *del_res = PQexecParams(conn, delete_sql, 1, NULL, delete_params, NULL, NULL, 0);
    if (PQresultStatus(del_res) != PGRES_COMMAND_OK) {
        if (error_out) {
            const char *errmsg = PQresultErrorMessage(del_res);
            *error_out = strdup(errmsg ? errmsg : "Failed to delete existing audit row");
        }
        PQclear(del_res);
        return 0;
    }
    PQclear(del_res);

    const char *insert_sql =
        "INSERT INTO audits ("
        "audit_uuid, form_id, form_name, form_version, submitted_on, submitted_by, updated_at, account_id, user_id, user_name, submit_guid, "
        "workflow_stage, workflow_user, building_address, building_owner, elevator_contractor, city_id, building_id, device_type, is_first_car, "
        "building_information, bank_name, cars_in_bank, total_floor_stop_names, machine_room_location, machine_room_location_other, "
        "controller_manufacturer, controller_model, controller_install_year, controller_type, controller_power_system, car_speed, dlm_compliant, "
        "maintenance_log_up_to_date, last_maintenance_log_date, code_data_plate_present, code_data_year, cat1_tag_current, cat1_tag_date, "
        "cat5_tag_current, cat5_tag_date, brake_tag_current, brake_tag_date, machine_manufacturer, machine_type, number_of_ropes, roping, "
        "rope_condition_score, motor_data_plate_present, motor_type, brake_type, single_or_dual_core_brake, rope_gripper_present, "
        "governor_manufacturer, governor_type, counterweight_governor, pump_motor_manufacturer, oil_condition, oil_level, valve_manufacturer, "
        "tank_heater_present, oil_cooler_present, capacity, door_operation, door_operation_type, number_of_openings, number_of_stops, floors_served, "
        "pi_type, rail_type, guide_type, car_door_equipment_manufacturer, car_door_lock_manufacturer, car_door_operator_manufacturer, "
        "car_door_operator_model, restrictor_type, has_hoistway_access_keyswitches, hallway_pi_type, hatch_door_unlocking_type, "
        "hatch_door_equipment_manufacturer, hatch_door_lock_manufacturer, pit_access, safety_type, buffer_type, sump_pump_present, "
        "compensation_type, jack_piston_type, scavenger_pump_present, general_notes, door_opening_width, rating_overall, expected_stop_count, "
        "mobile_device, mobile_app_name, mobile_app_version, mobile_app_type, mobile_sdk_release, mobile_memory_mb)"
        " VALUES ("
        "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,$61,$62,$63,$64,$65,$66,$67,$68,$69,$70,$71,$72,$73,$74,$75,$76,$77,$78,$79,$80,$81,$82,$83,$84,$85,$86,$87,$88,$89,$90,$91,$92,$93,$94,$95,$96,$97,$98)";

    const char *values[AUDIT_PARAM_COUNT];
    int lengths[AUDIT_PARAM_COUNT];
    int formats[AUDIT_PARAM_COUNT];
    memset(lengths, 0, sizeof(lengths));
    memset(formats, 0, sizeof(formats));

    AllocationList pool;
    allocation_list_init(&pool);

    char *cars_array = pg_array_from_string_array(&record->cars_in_bank);
    char *stops_array = pg_array_from_string_array(&record->total_floor_stop_names);
    char *floors_array = pg_array_from_string_array(&record->floors_served);
    if ((!cars_array && record->cars_in_bank.count > 0) || (!stops_array && record->total_floor_stop_names.count > 0) || (!floors_array && record->floors_served.count > 0)) {
        if (error_out) *error_out = strdup("Failed to allocate arrays for audit insert");
        free(cars_array);
        free(stops_array);
        free(floors_array);
        allocation_list_clear(&pool);
        return 0;
    }
    allocation_list_add(&pool, cars_array);
    allocation_list_add(&pool, stops_array);
    allocation_list_add(&pool, floors_array);

    int idx = 0;
    values[idx++] = record->audit_uuid;

    const char *form_id_param = optional_long_param(&record->form_id, &pool);
    if (record->form_id.has_value && !form_id_param) goto oom_params;
    values[idx++] = form_id_param;

    values[idx++] = record->form_name;

    const char *form_version_param = optional_int_param(&record->form_version, &pool);
    if (record->form_version.has_value && !form_version_param) goto oom_params;
    values[idx++] = form_version_param;

    values[idx++] = record->submitted_on;
    values[idx++] = record->submitted_by;
    values[idx++] = record->updated_at;

    const char *account_id_param = optional_long_param(&record->account_id, &pool);
    if (record->account_id.has_value && !account_id_param) goto oom_params;
    values[idx++] = account_id_param;

    const char *user_id_param = optional_long_param(&record->user_id, &pool);
    if (record->user_id.has_value && !user_id_param) goto oom_params;
    values[idx++] = user_id_param;

    values[idx++] = record->user_name;
    values[idx++] = record->submit_guid;
    values[idx++] = record->workflow_stage;
    values[idx++] = record->workflow_user;
    values[idx++] = record->building_address;
    values[idx++] = record->building_owner;
    values[idx++] = record->elevator_contractor;
    values[idx++] = record->city_id;
    values[idx++] = record->building_id;
    values[idx++] = record->device_type;
    values[idx++] = optional_bool_param(&record->is_first_car);
    values[idx++] = record->building_information;
    values[idx++] = record->bank_name;
    values[idx++] = cars_array;
    values[idx++] = stops_array;
    values[idx++] = record->machine_room_location;
    values[idx++] = record->machine_room_location_other;
    values[idx++] = record->controller_manufacturer;
    values[idx++] = record->controller_model;

    const char *controller_year_param = optional_int_param(&record->controller_install_year, &pool);
    if (record->controller_install_year.has_value && !controller_year_param) goto oom_params;
    values[idx++] = controller_year_param;

    values[idx++] = record->controller_type;
    values[idx++] = record->controller_power_system;

    const char *car_speed_param = optional_int_param(&record->car_speed, &pool);
    if (record->car_speed.has_value && !car_speed_param) goto oom_params;
    values[idx++] = car_speed_param;

    values[idx++] = optional_bool_param(&record->dlm_compliant);
    values[idx++] = optional_bool_param(&record->maintenance_log_up_to_date);
    values[idx++] = record->last_maintenance_log_date;
    values[idx++] = optional_bool_param(&record->code_data_plate_present);

    const char *code_year_param = optional_int_param(&record->code_data_year, &pool);
    if (record->code_data_year.has_value && !code_year_param) goto oom_params;
    values[idx++] = code_year_param;

    values[idx++] = optional_bool_param(&record->cat1_tag_current);
    values[idx++] = record->cat1_tag_date;
    values[idx++] = optional_bool_param(&record->cat5_tag_current);
    values[idx++] = record->cat5_tag_date;
    values[idx++] = optional_bool_param(&record->brake_tag_current);
    values[idx++] = record->brake_tag_date;
    values[idx++] = record->machine_manufacturer;
    values[idx++] = record->machine_type;

    const char *ropes_param = optional_int_param(&record->number_of_ropes, &pool);
    if (record->number_of_ropes.has_value && !ropes_param) goto oom_params;
    values[idx++] = ropes_param;

    values[idx++] = record->roping;

    const char *rope_condition_param = optional_int_param(&record->rope_condition_score, &pool);
    if (record->rope_condition_score.has_value && !rope_condition_param) goto oom_params;
    values[idx++] = rope_condition_param;

    values[idx++] = optional_bool_param(&record->motor_data_plate_present);
    values[idx++] = record->motor_type;
    values[idx++] = record->brake_type;
    values[idx++] = record->single_or_dual_core_brake;
    values[idx++] = optional_bool_param(&record->rope_gripper_present);
    values[idx++] = record->governor_manufacturer;
    values[idx++] = record->governor_type;
    values[idx++] = optional_bool_param(&record->counterweight_governor);
    values[idx++] = record->pump_motor_manufacturer;
    values[idx++] = record->oil_condition;
    values[idx++] = record->oil_level;
    values[idx++] = record->valve_manufacturer;
    values[idx++] = optional_bool_param(&record->tank_heater_present);
    values[idx++] = optional_bool_param(&record->oil_cooler_present);

    const char *capacity_param = optional_int_param(&record->capacity, &pool);
    if (record->capacity.has_value && !capacity_param) goto oom_params;
    values[idx++] = capacity_param;

    values[idx++] = record->door_operation;
    values[idx++] = record->door_operation_type;

    const char *openings_param = optional_int_param(&record->number_of_openings, &pool);
    if (record->number_of_openings.has_value && !openings_param) goto oom_params;
    values[idx++] = openings_param;

    const char *stops_param = optional_int_param(&record->number_of_stops, &pool);
    if (record->number_of_stops.has_value && !stops_param) goto oom_params;
    values[idx++] = stops_param;

    values[idx++] = floors_array;
    values[idx++] = record->pi_type;
    values[idx++] = record->rail_type;
    values[idx++] = record->guide_type;
    values[idx++] = record->car_door_equipment_manufacturer;
    values[idx++] = record->car_door_lock_manufacturer;
    values[idx++] = record->car_door_operator_manufacturer;
    values[idx++] = record->car_door_operator_model;
    values[idx++] = record->restrictor_type;
    values[idx++] = optional_bool_param(&record->has_hoistway_access_keyswitches);
    values[idx++] = record->hallway_pi_type;
    values[idx++] = record->hatch_door_unlocking_type;
    values[idx++] = record->hatch_door_equipment_manufacturer;
    values[idx++] = record->hatch_door_lock_manufacturer;
    values[idx++] = record->pit_access;
    values[idx++] = record->safety_type;
    values[idx++] = record->buffer_type;
    values[idx++] = optional_bool_param(&record->sump_pump_present);
    values[idx++] = record->compensation_type;
    values[idx++] = record->jack_piston_type;
    values[idx++] = optional_bool_param(&record->scavenger_pump_present);
    values[idx++] = record->general_notes;

    const char *door_width_param = optional_double_param(&record->door_opening_width, &pool);
    if (record->door_opening_width.has_value && !door_width_param) goto oom_params;
    values[idx++] = door_width_param;

    const char *rating_param = optional_int_param(&record->rating_overall, &pool);
    if (record->rating_overall.has_value && !rating_param) goto oom_params;
    values[idx++] = rating_param;

    const char *expected_param = optional_int_param(&record->expected_stop_count, &pool);
    if (record->expected_stop_count.has_value && !expected_param) goto oom_params;
    values[idx++] = expected_param;

    values[idx++] = record->mobile_device;
    values[idx++] = record->mobile_app_name;
    values[idx++] = record->mobile_app_version;
    values[idx++] = record->mobile_app_type;
    values[idx++] = record->mobile_sdk_release;

    const char *mobile_mem_param = optional_long_param(&record->mobile_memory_mb, &pool);
    if (record->mobile_memory_mb.has_value && !mobile_mem_param) goto oom_params;
    values[idx++] = mobile_mem_param;

    if (idx != AUDIT_PARAM_COUNT) {
        if (error_out) *error_out = strdup("Internal error: audit parameter count mismatch");
        allocation_list_clear(&pool);
        return 0;
    }

    PGresult *res = PQexecParams(conn, insert_sql, AUDIT_PARAM_COUNT, NULL, values, lengths, formats, 0);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        if (error_out) {
            const char *errmsg = PQresultErrorMessage(res);
            *error_out = strdup(errmsg ? errmsg : "Failed to insert audit record");
        }
        PQclear(res);
        allocation_list_clear(&pool);
        return 0;
    }
    PQclear(res);
    allocation_list_clear(&pool);
    return 1;

oom_params:
    if (error_out && !*error_out) {
        *error_out = strdup("Out of memory preparing audit insert");
    }
    allocation_list_clear(&pool);
    return 0;
}
static int db_replace_photos(PGconn *conn, const char *audit_uuid, const PhotoCollection *photos, const StringArray *photo_order, char **error_out) {
    const char *delete_sql = "DELETE FROM audit_photos WHERE audit_uuid = $1";
    const char *del_params[1] = { audit_uuid };
    PGresult *del_res = PQexecParams(conn, delete_sql, 1, NULL, del_params, NULL, NULL, 0);
    if (PQresultStatus(del_res) != PGRES_COMMAND_OK) {
        if (error_out) {
            const char *errmsg = PQresultErrorMessage(del_res);
            *error_out = strdup(errmsg ? errmsg : "Failed clearing existing photos");
        }
        PQclear(del_res);
        return 0;
    }
    PQclear(del_res);

    if (!photo_order || photo_order->count == 0) {
        return 1;
    }

    const char *insert_sql = "INSERT INTO audit_photos (audit_uuid, photo_filename, content_type, photo_bytes) VALUES ($1,$2,$3,$4)";

    for (size_t i = 0; i < photo_order->count; ++i) {
        const char *filename = photo_order->values[i];
        const PhotoFile *photo = photo_collection_find(photos, filename);
        if (!photo) {
            log_info("Photo %s listed in JSON but missing from archive", filename);
            continue;
        }
        const char *params[4];
        int lengths[4] = {0, 0, 0, (int)photo->size};
        int formats[4] = {0, 0, 0, 1};
        params[0] = audit_uuid;
        params[1] = photo->filename;
        params[2] = photo->content_type ? photo->content_type : "application/octet-stream";
        params[3] = (const char *)photo->data;

        PGresult *res = PQexecParams(conn, insert_sql, 4, NULL, params, lengths, formats, 0);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            if (error_out) {
                const char *errmsg = PQresultErrorMessage(res);
                *error_out = strdup(errmsg ? errmsg : "Failed inserting photo record");
            }
            PQclear(res);
            return 0;
        }
        PQclear(res);
    }
    return 1;
}

static int db_replace_deficiencies(PGconn *conn, const char *audit_uuid, const DeficiencyList *deficiencies, char **error_out) {
    ResolvedMap resolved_map;
    resolved_map_init(&resolved_map);

    const char *existing_sql =
        "SELECT overlay_code, violation_device_id, violation_equipment, violation_condition, violation_remedy, violation_note, resolved_at "
        "FROM audit_deficiencies WHERE audit_uuid = $1";
    const char *paramAudit[1] = { audit_uuid };
    PGresult *existing_res = PQexecParams(conn, existing_sql, 1, NULL, paramAudit, NULL, NULL, 0);
    if (PQresultStatus(existing_res) == PGRES_TUPLES_OK) {
        int rows = PQntuples(existing_res);
        for (int i = 0; i < rows; ++i) {
            const char *overlay = PQgetisnull(existing_res, i, 0) ? NULL : PQgetvalue(existing_res, i, 0);
            const char *device = PQgetisnull(existing_res, i, 1) ? NULL : PQgetvalue(existing_res, i, 1);
            const char *equipment = PQgetisnull(existing_res, i, 2) ? NULL : PQgetvalue(existing_res, i, 2);
            const char *condition = PQgetisnull(existing_res, i, 3) ? NULL : PQgetvalue(existing_res, i, 3);
            const char *remedy = PQgetisnull(existing_res, i, 4) ? NULL : PQgetvalue(existing_res, i, 4);
            const char *note = PQgetisnull(existing_res, i, 5) ? NULL : PQgetvalue(existing_res, i, 5);
            const char *resolved = PQgetisnull(existing_res, i, 6) ? NULL : PQgetvalue(existing_res, i, 6);
            char *key = build_deficiency_key(overlay, device, equipment, condition, remedy, note);
            if (!key) {
                resolved_map_clear(&resolved_map);
                PQclear(existing_res);
                if (error_out) *error_out = strdup("Out of memory");
                return 0;
            }
            if (!resolved_map_put(&resolved_map, key, resolved)) {
                free(key);
                resolved_map_clear(&resolved_map);
                PQclear(existing_res);
                if (error_out) *error_out = strdup("Out of memory");
                return 0;
            }
            free(key);
        }
    } else {
        log_error("Failed reading existing deficiencies: %s", PQresultErrorMessage(existing_res));
    }
    PQclear(existing_res);

    const char *delete_sql = "DELETE FROM audit_deficiencies WHERE audit_uuid = $1";
    const char *del_params[1] = { audit_uuid };
    PGresult *del_res = PQexecParams(conn, delete_sql, 1, NULL, del_params, NULL, NULL, 0);
    if (PQresultStatus(del_res) != PGRES_COMMAND_OK) {
        if (error_out) {
            const char *errmsg = PQresultErrorMessage(del_res);
            *error_out = strdup(errmsg ? errmsg : "Failed clearing existing deficiencies");
        }
        PQclear(del_res);
        resolved_map_clear(&resolved_map);
        return 0;
    }
    PQclear(del_res);

    if (!deficiencies || deficiencies->count == 0) {
        resolved_map_clear(&resolved_map);
        return 1;
    }

    const char *insert_sql =
        "INSERT INTO audit_deficiencies (audit_uuid, section_counter, violation_device_id, equipment_code, condition_code, remedy_code, overlay_code, violation_equipment, violation_condition, violation_remedy, violation_note, resolved_at) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)";

    for (size_t i = 0; i < deficiencies->count; ++i) {
        const Deficiency *d = &deficiencies->items[i];
        const char *params[12];
        int lengths[12] = {0};
        int formats[12] = {0};
        char section_buf[32];
        snprintf(section_buf, sizeof(section_buf), "%d", d->section_counter);

        char *key = build_deficiency_key(d->overlay_code, d->violation_device_id, d->violation_equipment, d->violation_condition, d->violation_remedy, d->violation_note);
        const char *resolved_existing = resolved_map_get(&resolved_map, key);
        free(key);

        params[0] = audit_uuid;
        params[1] = section_buf;
        params[2] = d->violation_device_id;
        params[3] = d->equipment_code;
        params[4] = d->condition_code;
        params[5] = d->remedy_code;
        params[6] = d->overlay_code;
        params[7] = d->violation_equipment;
        params[8] = d->violation_condition;
        params[9] = d->violation_remedy;
        params[10] = d->violation_note;
        params[11] = resolved_existing;

        PGresult *res = PQexecParams(conn, insert_sql, 12, NULL, params, lengths, formats, 0);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            if (error_out) {
                const char *errmsg = PQresultErrorMessage(res);
                *error_out = strdup(errmsg ? errmsg : "Failed inserting deficiency record");
            }
            PQclear(res);
            resolved_map_clear(&resolved_map);
            return 0;
        }
        PQclear(res);
    }

    resolved_map_clear(&resolved_map);
    return 1;
}

static int db_upsert_audit(PGconn *conn, const AuditRecord *record, const PhotoCollection *photos, const StringArray *photo_order, const DeficiencyList *deficiencies, char **error_out) {
    if (!db_begin(conn, error_out)) {
        return 0;
    }
    char *step_error = NULL;
    if (!db_insert_audit(conn, record, &step_error)) {
        db_rollback(conn);
        if (step_error) {
            if (error_out) *error_out = step_error; else free(step_error);
        }
        return 0;
    }
    if (!db_replace_photos(conn, record->audit_uuid, photos, photo_order, &step_error)) {
        db_rollback(conn);
        if (step_error) {
            if (error_out) *error_out = step_error; else free(step_error);
        }
        return 0;
    }
    if (!db_replace_deficiencies(conn, record->audit_uuid, deficiencies, &step_error)) {
        db_rollback(conn);
        if (step_error) {
            if (error_out) *error_out = step_error; else free(step_error);
        }
        return 0;
    }
    if (!db_commit(conn, error_out)) {
        db_rollback(conn);
        return 0;
    }
    return 1;
}

static int process_extracted_archive(char *temp_dir, PGconn *conn, StringArray *processed_audits, char **error_out) {
    if (!temp_dir || !conn || !processed_audits) {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid archive processing parameters");
        }
        if (temp_dir) {
            remove_directory_recursive(temp_dir);
            free(temp_dir);
        }
        return 0;
    }

    int success = 0;
    char *csv_path = NULL;
    char *json_path = NULL;
    PhotoCollection photos;
    char *csv_text = NULL;
    CsvFile csv_file;
    bool csv_parsed = false;
    char *csv_error = NULL;
    char *json_text = NULL;
    JsonValue *json_root = NULL;
    char *json_error = NULL;
    StringArray photo_order;
    bool photo_order_init = false;
    DeficiencyList deficiency_list;
    bool deficiency_init = false;

    if (!collect_files(temp_dir, &csv_path, &json_path, &photos)) {
        if (error_out && !*error_out) *error_out = strdup("Failed to collect extracted files");
        goto cleanup;
    }
    if (!csv_path || !json_path) {
        if (error_out && !*error_out) *error_out = strdup("CSV or JSON file missing in archive");
        goto cleanup;
    }

    if (!read_file_to_string(csv_path, &csv_text)) {
        if (error_out && !*error_out) *error_out = strdup("Failed to read CSV file");
        goto cleanup;
    }

    if (!csv_parse(csv_text, &csv_file, &csv_error)) {
        if (error_out && !*error_out) {
            *error_out = csv_error ? csv_error : strdup("Failed to parse CSV content");
            csv_error = NULL;
        }
        goto cleanup;
    }
    csv_parsed = true;

    if (!read_file_to_string(json_path, &json_text)) {
        if (error_out && !*error_out) *error_out = strdup("Failed to read JSON file");
        goto cleanup;
    }

    json_root = json_parse(json_text, &json_error);
    if (!json_root) {
        if (error_out && !*error_out) {
            *error_out = json_error ? json_error : strdup("Failed to parse JSON content");
            json_error = NULL;
        }
        goto cleanup;
    }

    if (!parse_photo_names(json_root, &photo_order)) {
        if (error_out && !*error_out) *error_out = strdup("Failed to parse photo list from JSON");
        goto cleanup;
    }
    photo_order_init = true;

    if (!parse_deficiencies(json_root, &deficiency_list)) {
        if (error_out && !*error_out) *error_out = strdup("Failed to parse deficiencies from JSON");
        goto cleanup;
    }
    deficiency_init = true;

    if (csv_file.row_count == 0) {
        if (error_out && !*error_out) *error_out = strdup("CSV file did not contain any audit rows");
        goto cleanup;
    }

    for (size_t i = 0; i < csv_file.row_count; ++i) {
        const CsvRow *row = &csv_file.rows[i];
        AuditRecord record;
        char *record_error = NULL;
        if (!populate_audit_record(&csv_file, row, json_root, &record, &record_error)) {
            if (record_error) {
                if (error_out && !*error_out) {
                    *error_out = record_error;
                    record_error = NULL;
                }
            } else if (error_out && !*error_out) {
                *error_out = strdup("Failed to populate audit record");
            }
            audit_record_free(&record);
            goto cleanup;
        }

        if (audit_exists(conn, record.audit_uuid)) {
            log_info("Audit %s already exists; overwriting with new data", record.audit_uuid);
        }

        char *upsert_error = NULL;
        if (!db_upsert_audit(conn, &record, &photos, &photo_order, &deficiency_list, &upsert_error)) {
            if (upsert_error) {
                if (error_out && !*error_out) {
                    *error_out = upsert_error;
                    upsert_error = NULL;
                }
            } else if (error_out && !*error_out) {
                *error_out = strdup("Database insert failed");
            }
            audit_record_free(&record);
            goto cleanup;
        }
        free(upsert_error);

        if (!string_array_append_copy(processed_audits, record.audit_uuid)) {
            if (error_out && !*error_out) *error_out = strdup("Failed recording processed audit id");
            audit_record_free(&record);
            goto cleanup;
        }

        audit_record_free(&record);
    }

    success = 1;

cleanup:
    if (deficiency_init) {
        deficiency_list_clear(&deficiency_list);
    }
    if (photo_order_init) {
        string_array_clear(&photo_order);
    }
    if (json_root) {
        json_free(json_root);
    }
    if (csv_parsed) {
        csv_free(&csv_file);
    }
    free(csv_text);
    free(json_text);
    free(csv_path);
    free(json_path);
    free(csv_error);
    free(json_error);
    photo_collection_clear(&photos);
    remove_directory_recursive(temp_dir);
    free(temp_dir);
    return success;
}

static void handle_options_request(int client_fd) {
    send_http_response(client_fd, 204, "No Content", "application/json", NULL, 0);
}

static char *build_deficiency_key(const char *overlay_code, const char *device_id, const char *equipment, const char *condition, const char *remedy, const char *note) {
    const char *parts[6] = {
        overlay_code && overlay_code[0] ? overlay_code : "",
        device_id && device_id[0] ? device_id : "",
        equipment && equipment[0] ? equipment : "",
        condition && condition[0] ? condition : "",
        remedy && remedy[0] ? remedy : "",
        note && note[0] ? note : ""
    };
    size_t total = 0;
    for (size_t i = 0; i < 6; ++i) {
        total += strlen(parts[i]);
    }
    total += 5; // delimiters
    char *key = malloc(total + 1);
    if (!key) {
        return NULL;
    }
    snprintf(key, total + 1, "%s|%s|%s|%s|%s|%s",
             parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]);
    return key;
}

static bool read_request_body(int client_fd,
                              const char *header_lines,
                              char *body_start,
                              size_t leftover,
                              char saved_body_char,
                              long max_length,
                              char **body_out,
                              long *length_out,
                              int *status_out,
                              const char **error_out) {
    if (body_out) *body_out = NULL;
    if (length_out) *length_out = 0;
    if (status_out) *status_out = 400;
    if (error_out) *error_out = NULL;

    long content_length = -1;
    const char *line = header_lines;
    while (line && *line) {
        const char *next = strstr(line, "\r\n");
        size_t len = next ? (size_t)(next - line) : strlen(line);
        if (len == 0) {
            break;
        }
        if (len >= 15 && strncasecmp(line, "Content-Length:", 15) == 0) {
            const char *value = line + 15;
            while (*value == ' ' || *value == '\t') value++;
            content_length = strtol(value, NULL, 10);
        }
        if (!next) {
            break;
        }
        line = next + 2;
    }

    if (content_length < 0) {
        if (status_out) *status_out = 411;
        if (error_out) *error_out = "Content-Length required";
        if (body_start) *body_start = saved_body_char;
        return false;
    }
    if (content_length > max_length) {
        if (status_out) *status_out = 400;
        if (error_out) *error_out = "Invalid request body length";
        if (body_start) *body_start = saved_body_char;
        return false;
    }

    char *body = malloc((size_t)content_length + 1);
    if (!body) {
        if (status_out) *status_out = 500;
        if (error_out) *error_out = "Out of memory";
        if (body_start) *body_start = saved_body_char;
        return false;
    }

    if (body_start) {
        *body_start = saved_body_char;
    }
    size_t offset = 0;
    if (leftover > 0 && body_start) {
        size_t copy_len = leftover > (size_t)content_length ? (size_t)content_length : leftover;
        memcpy(body, body_start, copy_len);
        offset += copy_len;
    }

    while ((long)offset < content_length) {
        ssize_t read_bytes = recv(client_fd, body + offset, (size_t)content_length - offset, 0);
        if (read_bytes <= 0) {
            free(body);
            if (status_out) *status_out = 400;
            if (error_out) *error_out = "Unexpected end of stream";
            return false;
        }
        offset += (size_t)read_bytes;
    }

    body[content_length] = '\0';
    if (body_out) *body_out = body;
    else free(body);
    if (length_out) *length_out = content_length;
    if (status_out) *status_out = 200;
    if (error_out) *error_out = NULL;
    return true;
}

static bool handle_zip_upload(int client_fd,
                              char *body_start,
                              size_t leftover,
                              char saved_body_char,
                              long content_length,
                              PGconn *conn,
                              StringArray *processed_audits,
                              int *status_out,
                              char **error_out) {
    if (status_out) {
        *status_out = 500;
    }
    if (error_out) {
        *error_out = NULL;
    }
    if (!conn || !processed_audits) {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid upload parameters");
        }
        if (status_out) {
            *status_out = 500;
        }
        if (body_start) {
            *body_start = saved_body_char;
        }
        return false;
    }

    if (content_length <= 0) {
        if (error_out && !*error_out) {
            *error_out = strdup("Content-Length must be positive");
        }
        if (status_out) {
            *status_out = 400;
        }
        if (body_start) {
            *body_start = saved_body_char;
        }
        return false;
    }

    if (leftover > (size_t)content_length) {
        if (error_out && !*error_out) {
            *error_out = strdup("Content-Length mismatch");
        }
        if (status_out) {
            *status_out = 400;
        }
        if (body_start) {
            *body_start = saved_body_char;
        }
        return false;
    }

    if (body_start) {
        *body_start = saved_body_char;
    }

    char *temp_dir = create_temp_dir();
    if (!temp_dir) {
        if (error_out && !*error_out) {
            *error_out = strdup("Failed to create temporary directory");
        }
        if (status_out) {
            *status_out = 500;
        }
        return false;
    }

    int pipefd[2];
    if (pipe(pipefd) != 0) {
        if (error_out && !*error_out) {
            char msg[160];
            snprintf(msg, sizeof(msg), "Failed to create unzip pipe: %s", strerror(errno));
            *error_out = strdup(msg);
        }
        if (status_out) {
            *status_out = 500;
        }
        remove_directory_recursive(temp_dir);
        free(temp_dir);
        return false;
    }

    pid_t pid = fork();
    if (pid < 0) {
        if (error_out && !*error_out) {
            char msg[160];
            snprintf(msg, sizeof(msg), "fork failed: %s", strerror(errno));
            *error_out = strdup(msg);
        }
        if (status_out) {
            *status_out = 500;
        }
        close(pipefd[0]);
        close(pipefd[1]);
        remove_directory_recursive(temp_dir);
        free(temp_dir);
        return false;
    }

    int write_fd = pipefd[1];
    if (pid == 0) {
        close(write_fd);
        if (dup2(pipefd[0], STDIN_FILENO) < 0) {
            _exit(127);
        }
        close(pipefd[0]);
        execlp("unzip", "unzip", "-qq", "-d", temp_dir, "-", (char *)NULL);
        _exit(127);
    }

    close(pipefd[0]);

    size_t total_written = 0;
    if (leftover > 0 && body_start) {
        size_t to_write = leftover;
        if ((long)to_write > content_length) {
            to_write = (size_t)content_length;
        }
        if (!write_all(write_fd, body_start, to_write)) {
            if (error_out && !*error_out) {
                *error_out = strdup("Failed writing request body");
            }
            if (status_out) {
                *status_out = 500;
            }
            close(write_fd);
            kill(pid, SIGTERM);
            waitpid(pid, NULL, 0);
            remove_directory_recursive(temp_dir);
            free(temp_dir);
            return false;
        }
        total_written += to_write;
        if ((long)leftover > content_length) {
            if (error_out && !*error_out) {
                *error_out = strdup("Content-Length mismatch");
            }
            if (status_out) {
                *status_out = 400;
            }
            close(write_fd);
            kill(pid, SIGTERM);
            waitpid(pid, NULL, 0);
            remove_directory_recursive(temp_dir);
            free(temp_dir);
            return false;
        }
    }

    char buffer[READ_BUFFER_SIZE];
    while (total_written < (size_t)content_length) {
        size_t remaining = (size_t)content_length - total_written;
        size_t chunk = remaining < sizeof(buffer) ? remaining : sizeof(buffer);
        ssize_t nread = recv(client_fd, buffer, chunk, 0);
        if (nread <= 0) {
            if (error_out && !*error_out) {
                *error_out = strdup("Unexpected end of stream");
            }
            if (status_out) {
                *status_out = 400;
            }
            close(write_fd);
            kill(pid, SIGTERM);
            waitpid(pid, NULL, 0);
            remove_directory_recursive(temp_dir);
            free(temp_dir);
            return false;
        }
        if (!write_all(write_fd, buffer, (size_t)nread)) {
            if (error_out && !*error_out) {
                *error_out = strdup("Failed writing request body");
            }
            if (status_out) {
                *status_out = 500;
            }
            close(write_fd);
            kill(pid, SIGTERM);
            waitpid(pid, NULL, 0);
            remove_directory_recursive(temp_dir);
            free(temp_dir);
            return false;
        }
        total_written += (size_t)nread;
    }

    close(write_fd);
    write_fd = -1;

    int unzip_status = 0;
    if (waitpid(pid, &unzip_status, 0) < 0) {
        if (error_out && !*error_out) {
            char msg[160];
            snprintf(msg, sizeof(msg), "Failed waiting for unzip process: %s", strerror(errno));
            *error_out = strdup(msg);
        }
        if (status_out) {
            *status_out = 500;
        }
        remove_directory_recursive(temp_dir);
        free(temp_dir);
        return false;
    }
    if (!WIFEXITED(unzip_status) || WEXITSTATUS(unzip_status) != 0) {
        if (error_out && !*error_out) {
            char msg[160];
            snprintf(msg, sizeof(msg), "Archive extraction failed (status %d)", unzip_status);
            *error_out = strdup(msg);
        }
        if (status_out) {
            *status_out = 400;
        }
        remove_directory_recursive(temp_dir);
        free(temp_dir);
        return false;
    }

    bool processed_ok = process_extracted_archive(temp_dir, conn, processed_audits, error_out);
    if (!processed_ok) {
        if (status_out && *status_out == 500) {
            *status_out = 500;
        }
        return false;
    }
    if (status_out) {
        *status_out = 200;
    }
    return true;
}

static char *create_temp_dir(void) {
    char *template = strdup(TEMP_DIR_TEMPLATE);
    if (!template) {
        return NULL;
    }
    if (!mkdtemp(template)) {
        log_error("mkdtemp failed: %s", strerror(errno));
        free(template);
        return NULL;
    }
    return template;
}

static char *sanitize_path_component(const char *input) {
    if (!input || !*input) {
        return strdup("device");
    }
    size_t len = strlen(input);
    char *out = malloc(len + 1);
    if (!out) {
        return NULL;
    }
    size_t pos = 0;
    for (size_t i = 0; i < len; ++i) {
        unsigned char c = (unsigned char)input[i];
        if (isalnum(c)) {
            out[pos++] = (char)c;
        } else if (c == ' ' || c == '-' || c == '_') {
            out[pos++] = c == ' ' ? '_' : (char)c;
        } else {
            out[pos++] = '_';
        }
    }
    if (pos == 0) {
        out[pos++] = 'd';
        out[pos++] = 'e';
        out[pos++] = 'v';
    }
    out[pos] = '\0';
    return out;
}

static int copy_file_contents(const char *src_path, const char *dst_path) {
    FILE *src = fopen(src_path, "rb");
    if (!src) {
        return -1;
    }
    FILE *dst = fopen(dst_path, "wb");
    if (!dst) {
        fclose(src);
        return -1;
    }
    char buffer[8192];
    size_t n;
    int error = 0;
    while ((n = fread(buffer, 1, sizeof(buffer), src)) > 0) {
        if (fwrite(buffer, 1, n, dst) != n) {
            error = 1;
            break;
        }
    }
    if (ferror(src)) {
        error = 1;
    }
    fclose(src);
    if (fclose(dst) != 0) {
        error = 1;
    }
    if (error) {
        unlink(dst_path);
        errno = EIO;
        return -1;
    }
    return 0;
}

static int export_building_photos(PGconn *conn, const ReportData *report, const char *root_dir, char **error_out) {
    if (!conn || !report || !root_dir) {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid export parameters");
        }
        return 0;
    }

    char *site_dir = join_path(root_dir, "SITE PICTURES");
    if (!site_dir) {
        if (error_out && !*error_out) {
            *error_out = strdup("Out of memory creating site pictures directory");
        }
        return 0;
    }
    if (ensure_directory_exists(site_dir) != 0) {
        if (error_out && !*error_out) {
            char msg[160];
            snprintf(msg, sizeof(msg), "Failed to create %s: %s", site_dir, strerror(errno));
            *error_out = strdup(msg);
        }
        free(site_dir);
        return 0;
    }

    const char *sql =
        "SELECT photo_filename, content_type, photo_bytes "
        "FROM audit_photos "
        "WHERE audit_uuid = $1::uuid";

    for (size_t i = 0; i < report->devices.count; ++i) {
        const ReportDevice *device = &report->devices.items[i];
        if (!device->audit_uuid) {
            continue;
        }

        const char *name_source = device->device_id ? device->device_id : (device->submission_id ? device->submission_id : device->audit_uuid);
        char *safe_name = sanitize_path_component(name_source);
        if (!safe_name) {
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory creating device directory");
            }
            free(site_dir);
            return 0;
        }
        char *device_dir = join_path(site_dir, safe_name);
        free(safe_name);
        if (!device_dir) {
            if (error_out && !*error_out) {
                *error_out = strdup("Out of memory creating device directory path");
            }
            free(site_dir);
            return 0;
        }
        if (ensure_directory_exists(device_dir) != 0) {
            if (error_out && !*error_out) {
                char msg[160];
                snprintf(msg, sizeof(msg), "Failed to create %s: %s", device_dir, strerror(errno));
                *error_out = strdup(msg);
            }
            free(device_dir);
            free(site_dir);
            return 0;
        }

        const char *params[1] = { device->audit_uuid };
        int resultFormat = 1; // binary for photo_bytes
        PGresult *res = PQexecParams(conn, sql, 1, NULL, params, NULL, NULL, resultFormat);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            if (error_out && !*error_out) {
                const char *msg = PQresultErrorMessage(res);
                *error_out = strdup(msg ? msg : "Failed to fetch device photos");
            }
            PQclear(res);
            free(device_dir);
            free(site_dir);
            return 0;
        }

        int rows = PQntuples(res);
        for (int row = 0; row < rows; ++row) {
            const char *filename = PQgetisnull(res, row, 0) ? NULL : PQgetvalue(res, row, 0);
            const char *orig_name = (filename && *filename) ? filename : NULL;
            char generated_name[64];
            if (!orig_name) {
                snprintf(generated_name, sizeof(generated_name), "photo-%d.jpg", row + 1);
                orig_name = generated_name;
            }
            char *base_name = sanitize_path_component(orig_name);
            if (!base_name) {
                if (error_out && !*error_out) {
                    *error_out = strdup("Out of memory creating photo filename");
                }
                PQclear(res);
                free(device_dir);
                free(site_dir);
                return 0;
            }

            const char *ext = strrchr(orig_name, '.');
            char *final_name = NULL;
            if (ext && *(ext + 1)) {
                size_t base_len = strlen(base_name);
                size_t ext_len = strlen(ext);
                final_name = malloc(base_len + ext_len + 1);
                if (!final_name) {
                    if (error_out && !*error_out) {
                        *error_out = strdup("Out of memory creating photo filename");
                    }
                    free(base_name);
                    PQclear(res);
                    free(device_dir);
                    free(site_dir);
                    return 0;
                }
                memcpy(final_name, base_name, base_len);
                memcpy(final_name + base_len, ext, ext_len + 1);
            } else {
                final_name = base_name;
                base_name = NULL;
            }

            char *photo_path = join_path(device_dir, final_name);
            if (!photo_path) {
                if (error_out && !*error_out) {
                    *error_out = strdup("Out of memory assembling photo path");
                }
                free(final_name);
                free(base_name);
                PQclear(res);
                free(device_dir);
                free(site_dir);
                return 0;
            }

            free(base_name);

            const unsigned char *bytes = (const unsigned char *)PQgetvalue(res, row, 2);
            size_t length = (size_t)PQgetlength(res, row, 2);
            FILE *fp = fopen(photo_path, "wb");
            if (!fp) {
                if (error_out && !*error_out) {
                    char msg[160];
                    snprintf(msg, sizeof(msg), "Failed to write %s: %s", photo_path, strerror(errno));
                    *error_out = strdup(msg);
                }
                free(photo_path);
                PQclear(res);
                free(device_dir);
                free(site_dir);
                free(final_name);
                return 0;
            }
            if (length > 0 && fwrite(bytes, 1, length, fp) != length) {
                if (error_out && !*error_out) {
                    char msg[160];
                    snprintf(msg, sizeof(msg), "Failed to write %s: %s", photo_path, "short write");
                    *error_out = strdup(msg);
                }
                fclose(fp);
                unlink(photo_path);
                free(photo_path);
                PQclear(res);
                free(device_dir);
                free(site_dir);
                free(final_name);
                return 0;
            }
            fclose(fp);
            free(photo_path);
            free(final_name);
        }
        PQclear(res);
        free(device_dir);
    }

    free(site_dir);
    return 1;
}

static int create_report_archive(PGconn *conn, const ReportJob *job, const ReportData *report,
                                 const char *job_dir, const char *pdf_path, char **zip_path_out, char **error_out) {
    if (zip_path_out) {
        *zip_path_out = NULL;
    }
    (void)job;
    if (!conn || !job_dir || !pdf_path) {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid archive parameters");
        }
        return 0;
    }

    char *package_dir = create_temp_dir();
    if (!package_dir) {
        if (error_out && !*error_out) {
            *error_out = strdup("Failed to create package directory");
        }
        return 0;
    }

    char *pdf_copy_path = join_path(package_dir, "audit_report.pdf");
    if (!pdf_copy_path || copy_file_contents(pdf_path, pdf_copy_path) != 0) {
        if (error_out && !*error_out) {
            *error_out = strdup("Failed to prepare report PDF");
        }
        free(pdf_copy_path);
        remove_directory_recursive(package_dir);
        free(package_dir);
        return 0;
    }

    char *photo_error = NULL;
    if (!export_building_photos(conn, report, package_dir, &photo_error)) {
        if (error_out && !*error_out) {
            *error_out = photo_error ? photo_error : strdup("Failed to export photos");
        } else {
            free(photo_error);
        }
        free(pdf_copy_path);
        remove_directory_recursive(package_dir);
        free(package_dir);
        return 0;
    }
    free(photo_error);
    free(pdf_copy_path);

    char *zip_path = join_path(job_dir, "audit_report_package.zip");
    if (!zip_path) {
        if (error_out && !*error_out) {
            *error_out = strdup("Out of memory creating zip path");
        }
        remove_directory_recursive(package_dir);
        free(package_dir);
        return 0;
    }

    pid_t pid = fork();
    if (pid < 0) {
        if (error_out && !*error_out) {
            *error_out = strdup("Failed to fork zip process");
        }
        free(zip_path);
        remove_directory_recursive(package_dir);
        free(package_dir);
        return 0;
    }
    if (pid == 0) {
        if (chdir(package_dir) != 0) {
            _exit(127);
        }
        execlp("zip", "zip", "-r", "-q", zip_path, ".", (char *)NULL);
        _exit(127);
    }

    int status = 0;
    if (waitpid(pid, &status, 0) < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        if (error_out && !*error_out) {
            *error_out = strdup("Failed to create report archive");
        }
        free(zip_path);
        remove_directory_recursive(package_dir);
        free(package_dir);
        return 0;
    }

    remove_directory_recursive(package_dir);
    free(package_dir);

    if (zip_path_out) {
        *zip_path_out = zip_path;
    } else {
        free(zip_path);
    }
    return 1;
}

static int process_report_job(PGconn *conn, const ReportJob *job, char **output_path_out, char **error_out) {
    if (output_path_out) {
        *output_path_out = NULL;
    }
    if (!conn || !job || !job->address || !g_report_output_dir) {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid report job");
        }
        return 0;
    }

    int success = 0;
    ReportData report;
    report_data_init(&report);

    char *job_dir = NULL;
    char *tex_path = NULL;
    char *pdf_path = NULL;
    char *zip_path = NULL;
    char *report_json = NULL;
    NarrativeSet narratives;
    narrative_set_init(&narratives);

    const size_t SECTION_COUNT = 6;
    NarrativeTask tasks[6];
    memset(tasks, 0, sizeof(tasks));
    size_t section_count = SECTION_COUNT;
    size_t tasks_created = 0;
    char *narrative_build_error = NULL;

    char *load_error = NULL;
    if (!load_report_for_building(conn, job->address, &report, &load_error)) {
        if (error_out && !*error_out) {
            *error_out = load_error ? load_error : strdup("Failed to load report data");
        } else {
            free(load_error);
        }
        goto cleanup;
    }
    free(load_error);

    job_dir = join_path(g_report_output_dir, job->job_id);
    if (!job_dir) {
        if (error_out && !*error_out) {
            *error_out = strdup("Out of memory");
        }
        goto cleanup;
    }
    if (ensure_directory_exists(job_dir) != 0) {
        if (error_out && !*error_out) {
            char *msg = malloc(128);
            if (msg) {
                snprintf(msg, 128, "Failed to prepare report directory %s: %s", job_dir, strerror(errno));
                *error_out = msg;
            }
        }
        goto cleanup;
    }

    if (g_report_assets_dir && g_report_assets_dir[0]) {
        const char *asset_files[] = {"citywide.png", "square.png"};
        for (size_t i = 0; i < sizeof(asset_files) / sizeof(asset_files[0]); ++i) {
            char *src_path = join_path(g_report_assets_dir, asset_files[i]);
            char *dst_path = join_path(job_dir, asset_files[i]);
            if (!src_path || !dst_path) {
                if (error_out && !*error_out) {
                    *error_out = strdup("Out of memory preparing report assets");
                }
                free(src_path);
                free(dst_path);
                goto cleanup;
            }
            if (copy_file_contents(src_path, dst_path) != 0) {
                if (error_out && !*error_out) {
                    char *msg = malloc(160);
                    if (msg) {
                        snprintf(msg, 160, "Failed to copy asset %s: %s", asset_files[i], strerror(errno));
                        *error_out = msg;
                    }
                }
                free(src_path);
                free(dst_path);
                goto cleanup;
            }
            free(src_path);
            free(dst_path);
        }
    }

    tex_path = join_path(job_dir, "audit_report.tex");
    pdf_path = join_path(job_dir, "audit_report.pdf");
    if (!tex_path || !pdf_path) {
        if (error_out && !*error_out) {
            *error_out = strdup("Out of memory");
        }
        goto cleanup;
    }

    report_json = report_data_to_json(&report);
    if (!report_json) {
        if (error_out && !*error_out) {
            *error_out = strdup("Failed to serialize report data");
        }
        goto cleanup;
    }

    const char *system_prompt = "You are an expert vertical transportation safety consultant. Provide concise, professional narrative text suitable for a building owner. Use plain ASCII punctuation (no smart quotes or em dashes). Do not include LaTeX syntax or markdown.";

    struct {
        const char *title;
        char **slot;
        const char *instructions;
        bool include_notes;
        bool include_recommendations;
    } sections[] = {
        {"Executive Summary", &narratives.executive_summary,
         "Write an executive summary highlighting equipment condition, total device count, total deficiencies, and the most critical safety issues.\nProvide actionable context appropriate for ownership decisions.", true, false},
        {"Key Findings", &narratives.key_findings,
         "List the top findings from the audit with concise explanations. Focus on safety, compliance, and maintenance trends across devices.", true, false},
        {"Methodology", &narratives.methodology,
         "Describe the inspection methodology, standards referenced, and scope of the audit. Mention any limitations or assumptions.", false, false},
        {"Maintenance Performance", &narratives.maintenance_performance,
         "Analyze maintenance performance and recurring issues observed in the audit. Discuss patterns tied to equipment age, usage, or contractor performance.", true, false},
        {"Recommendations", &narratives.recommendations,
         "Provide prioritized recommendations for remediation, including immediate safety concerns, short-term actions, and long-term planning guidance.", true, true},
        {"Conclusion", &narratives.conclusion,
         "Deliver a closing narrative summarizing risk outlook, benefits of addressing recommendations, and next steps for maintaining compliance.", false, false}
    };

    section_count = sizeof(sections) / sizeof(sections[0]);

    for (size_t i = 0; i < section_count; ++i) {
        Buffer prompt;
        if (!buffer_init(&prompt)) {
            if (!narrative_build_error) {
                narrative_build_error = strdup("Out of memory");
            }
            goto narrative_join;
        }

        if (!buffer_appendf(&prompt, "%s\n\nAudit Data:\n%s", sections[i].instructions, report_json)) {
            buffer_free(&prompt);
            if (!narrative_build_error) {
                narrative_build_error = strdup("Out of memory");
            }
            goto narrative_join;
        }

        if (sections[i].include_notes && job->notes && job->notes[0]) {
            if (!buffer_appendf(&prompt, "\n\nInspector Notes:\n%s", job->notes)) {
                buffer_free(&prompt);
                if (!narrative_build_error) {
                    narrative_build_error = strdup("Out of memory");
                }
                goto narrative_join;
            }
        }
        if (sections[i].include_recommendations && job->recommendations && job->recommendations[0]) {
            if (!buffer_appendf(&prompt, "\n\nClient Guidance:\n%s", job->recommendations)) {
                buffer_free(&prompt);
                if (!narrative_build_error) {
                    narrative_build_error = strdup("Out of memory");
                }
                goto narrative_join;
            }
        }

        char *prompt_str = prompt.data;
        prompt.data = NULL;
        buffer_free(&prompt);
        if (!prompt_str) {
            if (!narrative_build_error) {
                narrative_build_error = strdup("Out of memory");
            }
            goto narrative_join;
        }

        tasks[i].system_prompt = system_prompt;
        tasks[i].prompt = prompt_str;
        tasks[i].slot = sections[i].slot;
        tasks[i].error = NULL;
        tasks[i].success = 0;
        tasks[i].thread_started = false;

        if (pthread_create(&tasks[i].thread, NULL, narrative_thread_main, &tasks[i]) == 0) {
            tasks[i].thread_started = true;
        } else {
            narrative_task_execute(&tasks[i]);
        }
        tasks_created = i + 1;
    }

narrative_join:
    for (size_t i = 0; i < tasks_created; ++i) {
        if (tasks[i].thread_started) {
            pthread_join(tasks[i].thread, NULL);
            tasks[i].thread_started = false;
        }
    }

    if (narrative_build_error) {
        if (error_out && !*error_out) {
            *error_out = narrative_build_error;
        } else {
            free(narrative_build_error);
        }
        narrative_build_error = NULL;
        goto cleanup;
    }

    for (size_t i = 0; i < tasks_created; ++i) {
        if (!tasks[i].success) {
            if (error_out && !*error_out) {
                if (tasks[i].error) {
                    *error_out = tasks[i].error;
                    tasks[i].error = NULL;
                } else {
                    *error_out = strdup("Narrative generation failed");
                }
            } else {
                free(tasks[i].error);
                tasks[i].error = NULL;
            }
            goto cleanup;
        }
    }

    char *latex_error = NULL;
    if (!build_report_latex(&report, &narratives, job, tex_path, &latex_error)) {
        if (error_out && !*error_out) {
            *error_out = latex_error ? latex_error : strdup("Failed to build LaTeX");
        } else {
            free(latex_error);
        }
        goto cleanup;
    }
    free(latex_error);

    char *compile_error = NULL;
    if (!run_pdflatex(job_dir, "audit_report.tex", &compile_error)) {
        if (error_out && !*error_out) {
            *error_out = compile_error ? compile_error : strdup("Failed compiling PDF");
        } else {
            free(compile_error);
        }
        goto cleanup;
    }
    free(compile_error);

    char *archive_error = NULL;
    if (!create_report_archive(conn, job, &report, job_dir, pdf_path, &zip_path, &archive_error)) {
        if (error_out && !*error_out) {
            *error_out = archive_error ? archive_error : strdup("Failed to build report archive");
        } else {
            free(archive_error);
        }
        goto cleanup;
    }
    free(archive_error);

    if (output_path_out) {
        *output_path_out = zip_path;
        zip_path = NULL;
    }
    success = 1;

cleanup:
    if (!success && output_path_out && *output_path_out) {
        free(*output_path_out);
        *output_path_out = NULL;
    }
    for (size_t i = 0; i < section_count; ++i) {
        free(tasks[i].prompt);
        tasks[i].prompt = NULL;
        free(tasks[i].error);
        tasks[i].error = NULL;
    }
    free(zip_path);
    free(report_json);
    narrative_set_clear(&narratives);
    report_data_clear(&report);
    free(job_dir);
    free(tex_path);
    if (pdf_path) {
        free(pdf_path);
    }
    return success;
}

static void signal_report_worker(void) {
    pthread_mutex_lock(&g_report_mutex);
    g_report_signal = true;
    pthread_cond_signal(&g_report_cond);
    pthread_mutex_unlock(&g_report_mutex);
}

static void *report_worker_main(void *arg) {
    (void)arg;
    PGconn *conn = NULL;

    for (;;) {
        pthread_mutex_lock(&g_report_mutex);
        bool stop_requested = g_report_stop;
        pthread_mutex_unlock(&g_report_mutex);
        if (stop_requested) {
            break;
        }

        if (!g_database_dsn) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 5;
            pthread_mutex_lock(&g_report_mutex);
            if (!g_report_stop) {
                pthread_cond_timedwait(&g_report_cond, &g_report_mutex, &ts);
            }
            g_report_signal = false;
            pthread_mutex_unlock(&g_report_mutex);
            continue;
        }

        if (!conn) {
            conn = PQconnectdb(g_database_dsn);
            if (PQstatus(conn) != CONNECTION_OK) {
                log_error("Report worker failed to connect to database: %s", PQerrorMessage(conn));
                PQfinish(conn);
                conn = NULL;
                sleep(5);
                continue;
            }
        }

        ReportJob job;
        report_job_init(&job);
        char *claim_error = NULL;
        int claimed = db_claim_next_report_job(conn, &job, &claim_error);
        if (claimed < 0) {
            log_error("Failed to claim report job: %s", claim_error ? claim_error : "unknown error");
            free(claim_error);
            report_job_clear(&job);
            PQfinish(conn);
            conn = NULL;
            sleep(2);
            continue;
        }
        free(claim_error);

        if (claimed == 0) {
            report_job_clear(&job);
            pthread_mutex_lock(&g_report_mutex);
            if (!g_report_stop) {
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                ts.tv_sec += 5;
                pthread_cond_timedwait(&g_report_cond, &g_report_mutex, &ts);
            }
            g_report_signal = false;
            stop_requested = g_report_stop;
            pthread_mutex_unlock(&g_report_mutex);
            if (stop_requested) {
                break;
            }
            continue;
        }

        log_info("Processing report job %s for %s", job.job_id, job.address ? job.address : "(unknown address)");

        char *output_path = NULL;
        char *process_error = NULL;
        int success = process_report_job(conn, &job, &output_path, &process_error);
        char *update_error = NULL;
        if (success) {
            if (!db_complete_report_job(conn, job.job_id, "completed", NULL, output_path, &update_error)) {
                log_error("Failed to mark report job %s completed: %s", job.job_id, update_error ? update_error : "unknown error");
            } else {
                log_info("Report job %s completed", job.job_id);
            }
        } else {
            const char *message = process_error ? process_error : "Report generation failed";
            if (!db_complete_report_job(conn, job.job_id, "failed", message, NULL, &update_error)) {
                log_error("Failed to mark report job %s failed: %s", job.job_id, update_error ? update_error : "unknown error");
            } else {
                log_error("Report job %s failed: %s", job.job_id, message);
            }
        }
        free(update_error);
        free(process_error);
        free(output_path);
        report_job_clear(&job);
    }

    if (conn) {
        PQfinish(conn);
    }
    return NULL;
}

static int run_pdflatex(const char *working_dir, const char *tex_filename, char **error_out) {
    if (!working_dir || !tex_filename) {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid LaTeX arguments");
        }
        return 0;
    }
    for (int pass = 0; pass < 3; ++pass) {
        pid_t pid = fork();
        if (pid == 0) {
            if (chdir(working_dir) != 0) {
                _exit(1);
            }
            execlp("pdflatex", "pdflatex", "-interaction=nonstopmode", tex_filename, (char *)NULL);
            _exit(1);
        } else if (pid < 0) {
            if (error_out && !*error_out) {
                *error_out = strdup("Failed to spawn pdflatex");
            }
            return 0;
        }

        int status = 0;
        if (waitpid(pid, &status, 0) < 0) {
            if (error_out && !*error_out) {
                *error_out = strdup("pdflatex wait failed");
            }
            return 0;
        }
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            if (error_out && !*error_out) {
                char *msg = malloc(64);
                if (msg) {
                    snprintf(msg, 64, "pdflatex failed on pass %d", pass + 1);
                    *error_out = msg;
                }
            }
            return 0;
        }
    }
    return 1;
}

static char *make_pgf_identifier(const char *label) {
    const char *src = label ? label : "item";
    char *clean = sanitize_ascii(src);
    const char *text = clean ? clean : src;
    size_t len = strlen(text);
    char *out = malloc(len + 1);
    if (!out) {
        free(clean);
        return NULL;
    }
    size_t pos = 0;
    for (size_t i = 0; i < len; ++i) {
        unsigned char c = (unsigned char)text[i];
        if (isalnum(c)) {
            out[pos++] = (char)tolower(c);
        } else if (c == ' ' || c == '-' || c == '/' || c == '_' || c == '+') {
            out[pos++] = '_';
        }
    }
    if (pos == 0) {
        out[pos++] = 'i';
        out[pos++] = 't';
        out[pos++] = 'e';
        out[pos++] = 'm';
    }
    out[pos] = '\0';
    free(clean);
    return out;
}

static int append_deficiency_code_chart(Buffer *buf, const ReportData *report) {
    if (!report || !buf) {
        return 0;
    }
    if (report->summary.deficiencies_by_code.count == 0) {
        return buffer_append_cstr(buf, "\\paragraph{Deficiencies by Condition Code}\\textit{No deficiencies recorded.}\\\n\n");
    }

    Buffer tokens, labels, coords;
    if (!buffer_init(&tokens) || !buffer_init(&labels) || !buffer_init(&coords)) {
        buffer_free(&tokens);
        buffer_free(&labels);
        buffer_free(&coords);
        return 0;
    }

    for (size_t i = 0; i < report->summary.deficiencies_by_code.count; ++i) {
        KeyCountEntry *entry = &report->summary.deficiencies_by_code.items[i];
        char *identifier = make_pgf_identifier(entry->key);
        char *label_tex = latex_escape(entry->key ? entry->key : "Unknown");
        if (!identifier || !label_tex) {
            free(identifier);
            free(label_tex);
            buffer_free(&tokens);
            buffer_free(&labels);
            buffer_free(&coords);
            return 0;
        }
        if (i > 0) {
            buffer_append_cstr(&tokens, ",");
            buffer_append_cstr(&labels, ",");
            buffer_append_cstr(&coords, " ");
        }
        buffer_appendf(&tokens, "%s", identifier);
        buffer_appendf(&labels, "%s", label_tex);
        buffer_appendf(&coords, "(%s,%d)", identifier, entry->count);
        free(identifier);
        free(label_tex);
    }

    int ok = 1;
    ok = ok && buffer_append_cstr(buf,
        "\\paragraph{Deficiencies by Condition Code}\n"
        "\\begin{figure}[H]\n"
        "\\centering\n"
        "\\begin{tikzpicture}\n"
        "\\begin{axis}[\n"
        "ybar,\n"
        "bar width=14pt,\n"
        "width=\\textwidth,\n"
        "height=7cm,\n"
        "xlabel={Condition Code},\n"
        "ylabel={Deficiencies},\n"
        "symbolic x coords={");
    ok = ok && buffer_appendf(buf, "%s", tokens.data ? tokens.data : "");
    ok = ok && buffer_append_cstr(buf, "},\n");
    ok = ok && buffer_append_cstr(buf, "xtick=data,\n");
    ok = ok && buffer_append_cstr(buf, "xticklabels={");
    ok = ok && buffer_appendf(buf, "%s", labels.data ? labels.data : "");
    ok = ok && buffer_append_cstr(buf,
        "},\n"
        "xticklabel style={rotate=45, anchor=east},\n"
        "ymin=0,\n"
        "ymajorgrids,\n"
        "nodes near coords,\n"
        "nodes near coords align={vertical}\n"
        "]\n");
    ok = ok && buffer_append_cstr(buf, "\\addplot coordinates {");
    ok = ok && buffer_appendf(buf, "%s", coords.data ? coords.data : "");
    ok = ok && buffer_append_cstr(buf, "};\n\\end{axis}\n\\end{tikzpicture}\n\\caption{Deficiencies by Condition Code}\n\\end{figure}\n\n");

    buffer_free(&tokens);
    buffer_free(&labels);
    buffer_free(&coords);
    return ok;
}

static int append_deficiencies_per_device_chart(Buffer *buf, const ReportData *report) {
    if (!buf || !report) {
        return 0;
    }

    if (report->devices.count == 0) {
        return buffer_append_cstr(buf, "\\paragraph{Deficiencies per Device}\\textit{No devices available.}\\\n\n");
    }

    Buffer tokens, labels, coords;
    if (!buffer_init(&tokens) || !buffer_init(&labels) || !buffer_init(&coords)) {
        buffer_free(&tokens);
        buffer_free(&labels);
        buffer_free(&coords);
        return 0;
    }

    for (size_t i = 0; i < report->devices.count; ++i) {
        ReportDevice *device = &report->devices.items[i];
        const char *id_src = device->device_id ? device->device_id :
                             (device->submission_id ? device->submission_id : device->audit_uuid);
        char *identifier = make_pgf_identifier(id_src);
        char *label_tex = latex_escape(id_src ? id_src : "Device");
        if (!identifier || !label_tex) {
            free(identifier);
            free(label_tex);
            buffer_free(&tokens);
            buffer_free(&labels);
            buffer_free(&coords);
            return 0;
        }
        if (i > 0) {
            buffer_append_cstr(&tokens, ",");
            buffer_append_cstr(&labels, ",");
            buffer_append_cstr(&coords, " ");
        }
        buffer_appendf(&tokens, "%s", identifier);
        buffer_appendf(&labels, "%s", label_tex);
        buffer_appendf(&coords, "(%s,%zu)", identifier, device->deficiencies.count);
        free(identifier);
        free(label_tex);
    }

    int ok = 1;
    ok = ok && buffer_append_cstr(buf,
        "\\paragraph{Deficiencies per Device}\n"
        "\\begin{figure}[H]\n\\centering\n\\begin{tikzpicture}\n\\begin{axis}[\n"
        "ybar,\n"
        "bar width=14pt,\n"
        "width=\\textwidth,\n"
        "height=7cm,\n"
        "xlabel={Device},\n"
        "ylabel={Deficiencies},\n"
        "symbolic x coords={");
    ok = ok && buffer_appendf(buf, "%s", tokens.data ? tokens.data : "");
    ok = ok && buffer_append_cstr(buf, "},\n");
    ok = ok && buffer_append_cstr(buf, "xtick=data,\n");
    ok = ok && buffer_append_cstr(buf, "xticklabels={");
    ok = ok && buffer_appendf(buf, "%s", labels.data ? labels.data : "");
    ok = ok && buffer_append_cstr(buf,
        "},\n"
        "xticklabel style={rotate=45, anchor=east},\n"
        "ymin=0,\n"
        "ymajorgrids,\n"
        "nodes near coords,\n"
        "nodes near coords align={vertical}\n"
        "]\n");
    ok = ok && buffer_append_cstr(buf, "\\addplot coordinates {");
    ok = ok && buffer_appendf(buf, "%s", coords.data ? coords.data : "");
    ok = ok && buffer_append_cstr(buf, "};\n\\end{axis}\n\\end{tikzpicture}\n\\caption{Deficiencies per Device}\n\\end{figure}\n\n");

    buffer_free(&tokens);
    buffer_free(&labels);
    buffer_free(&coords);
    return ok;
}

static int append_controller_age_chart(Buffer *buf, const ReportData *report) {
    if (!buf || !report) {
        return 0;
    }
    Buffer coords;
    if (!buffer_init(&coords)) {
        return 0;
    }
    size_t point_count = 0;
    for (size_t i = 0; i < report->devices.count; ++i) {
        ReportDevice *device = &report->devices.items[i];
        if (device->metrics.controller_age.has_value) {
            if (point_count > 0) {
                buffer_append_cstr(&coords, " ");
            }
            buffer_appendf(&coords, "(%d,%zu)", device->metrics.controller_age.value, device->deficiencies.count);
            point_count++;
        }
    }
    if (point_count == 0) {
        buffer_free(&coords);
        return buffer_append_cstr(buf, "\\paragraph{Controller Age vs Deficiencies}\\textit{Controller age data unavailable.}\\\n\n");
    }

    int ok = 1;
    ok = ok && buffer_append_cstr(buf, "\\paragraph{Controller Age vs Deficiencies}\n");
    ok = ok && buffer_append_cstr(buf,
        "\\begin{figure}[H]\n\\centering\n\\begin{tikzpicture}\n\\begin{axis}[\n"
        "width=\\textwidth,\n"
        "height=7cm,\n"
        "xlabel={Controller Age (Years)},\n"
        "ylabel={Documented Deficiencies},\n"
        "xmin=0,\n"
        "ymin=0,\n"
        "xmajorgrids,\n"
        "ymajorgrids\n]\\addplot[only marks, mark=*, mark size=2pt, color=tabblue] coordinates {");
    ok = ok && buffer_appendf(buf, "%s", coords.data ? coords.data : "");
    ok = ok && buffer_append_cstr(buf, "};\n\\end{axis}\n\\end{tikzpicture}\n\\caption{Controller Age vs Number of Deficiencies}\n\\end{figure}\n\n");

    buffer_free(&coords);
    return ok;
}

static int append_device_sections(Buffer *buf, const ReportData *report) {
    if (!buf || !report) {
        return 0;
    }

    if (!buffer_append_cstr(buf, "\\subsection{Per-Device Equipment Condition}\n\n")) {
        return 0;
    }

    const char *current_type = NULL;
    for (size_t i = 0; i < report->devices.count; ++i) {
        ReportDevice *device = &report->devices.items[i];
        const char *device_type = device->device_type ? device->device_type : "Device";
        char *device_type_clean = sanitize_ascii(device_type);
        const char *device_type_text = device_type_clean ? device_type_clean : device_type;
        char *device_type_tex = latex_escape(device_type_text);
        free(device_type_clean);

        const char *id_src = device->device_id ? device->device_id :
                            (device->submission_id ? device->submission_id : device->audit_uuid);
        char *device_id_clean = sanitize_ascii(id_src);
        const char *device_id_text = device_id_clean ? device_id_clean : (id_src ? id_src : "Device");
        char *device_id_tex = latex_escape(device_id_text);
        free(device_id_clean);

        if (!device_type_tex || !device_id_tex) {
            free(device_type_tex);
            free(device_id_tex);
            return 0;
        }

        if (!current_type || strcmp(current_type, device_type) != 0) {
            if (!buffer_appendf(buf, "\\paragraph{%ss}\n\n", device_type_tex)) {
                free(device_type_tex);
                free(device_id_tex);
                return 0;
            }
            current_type = device_type;
        }

        if (!buffer_appendf(buf, "\\subsubsection{%s %s}\n\n", device_type_tex, device_id_tex)) {
            free(device_type_tex);
            free(device_id_tex);
            return 0;
        }

        free(device_type_tex);
        free(device_id_tex);

        if (!buffer_append_cstr(buf, "\\begin{tabularx}{\\textwidth}{@{}lX@{}}\\toprule\n")) {
            return 0;
        }

        const struct {
            const char *label;
            const char *value;
        } info_rows[] = {
            {"Bank", device->bank_name},
            {"City ID", device->city_id},
            {"Controller Manufacturer", device->controller_manufacturer},
            {"Controller Model", device->controller_model},
            {"Machine Manufacturer", device->machine_manufacturer},
            {"Machine Type", device->machine_type},
            {"Roping", device->roping},
            {"Door Operation", device->door_operation}
        };

        for (size_t j = 0; j < sizeof(info_rows) / sizeof(info_rows[0]); ++j) {
            if (!info_rows[j].value || info_rows[j].value[0] == '\0') {
                continue;
            }
            char *label_clean = sanitize_ascii(info_rows[j].label);
            char *value_clean = sanitize_ascii(info_rows[j].value);
            const char *label_text = label_clean ? label_clean : info_rows[j].label;
            const char *value_text = value_clean ? value_clean : info_rows[j].value;
            char *label_tex = latex_escape(label_text);
            char *value_tex = latex_escape(value_text);
            free(label_clean);
            free(value_clean);
            if (!label_tex || !value_tex) {
                free(label_tex);
                free(value_tex);
                return 0;
            }
            if (!buffer_appendf(buf, "%s & %s \\\\ \n", label_tex, value_tex)) {
                free(label_tex);
                free(value_tex);
                return 0;
            }
            free(label_tex);
            free(value_tex);
        }

        struct {
            const char *label;
            const OptionalInt *value;
        } numeric_rows[] = {
            {"Capacity", &device->metrics.capacity},
            {"Car Speed", &device->metrics.car_speed},
            {"Controller Installation Year", &device->metrics.controller_install_year},
            {"Number of Stops", &device->metrics.number_of_stops},
            {"Code Data Year", &device->metrics.code_data_year}
        };

        char numeric[32];
        for (size_t j = 0; j < sizeof(numeric_rows) / sizeof(numeric_rows[0]); ++j) {
            const char *value_text = optional_int_to_text(numeric_rows[j].value, numeric, sizeof(numeric));
            if (!value_text || value_text[0] == '\0' || strcmp(value_text, "—") == 0) {
                continue;
            }
            char *label_tex = latex_escape(numeric_rows[j].label);
            char *value_tex = latex_escape(value_text);
            if (!label_tex || !value_tex || !buffer_appendf(buf, "%s & %s \\\\ \n", label_tex, value_tex)) {
                free(label_tex);
                free(value_tex);
                return 0;
            }
            free(label_tex);
            free(value_tex);
        }

        const char *bool_text = optional_bool_to_text(&device->metrics.dlm_compliant);
        if (bool_text && bool_text[0]) {
            char *label_tex = latex_escape("DLM Compliant");
            char *value_tex = latex_escape(bool_text);
            if (!label_tex || !value_tex || !buffer_appendf(buf, "%s & %s \\\\ \n", label_tex, value_tex)) {
                free(label_tex);
                free(value_tex);
                return 0;
            }
            free(label_tex);
            free(value_tex);
        }

        bool_text = optional_bool_to_text(&device->metrics.cat1_tag_current);
        if (bool_text && bool_text[0]) {
            char *label_tex = latex_escape("Cat 1 Tag Current");
            char *value_tex = latex_escape(bool_text);
            if (!label_tex || !value_tex || !buffer_appendf(buf, "%s & %s \\\\ \n", label_tex, value_tex)) {
                free(label_tex);
                free(value_tex);
                return 0;
            }
            free(label_tex);
            free(value_tex);
        }

        bool_text = optional_bool_to_text(&device->metrics.cat5_tag_current);
        if (bool_text && bool_text[0]) {
            char *label_tex = latex_escape("Cat 5 Tag Current");
            char *value_tex = latex_escape(bool_text);
            if (!label_tex || !value_tex || !buffer_appendf(buf, "%s & %s \\\\ \n", label_tex, value_tex)) {
                free(label_tex);
                free(value_tex);
                return 0;
            }
            free(label_tex);
            free(value_tex);
        }

        bool_text = optional_bool_to_text(&device->metrics.maintenance_log_up_to_date);
        if (bool_text && bool_text[0]) {
            char *label_tex = latex_escape("Maintenance Log Up to Date");
            char *value_tex = latex_escape(bool_text);
            if (!label_tex || !value_tex || !buffer_appendf(buf, "%s & %s \\\\ \n", label_tex, value_tex)) {
                free(label_tex);
                free(value_tex);
                return 0;
            }
            free(label_tex);
            free(value_tex);
        }

        if (!buffer_append_cstr(buf, "\\bottomrule\n\\end{tabularx}\n\n")) {
            return 0;
        }

        const char *notes_src = (device->general_notes && device->general_notes[0]) ? device->general_notes : "No general notes for this device.";
        char *notes_clean = sanitize_ascii(notes_src);
        const char *notes_text = notes_clean ? notes_clean : notes_src;
        char *notes_tex = latex_escape(notes_text);
        free(notes_clean);
        if (!notes_tex) {
            return 0;
        }
        if (!buffer_appendf(buf, "\\paragraph{General Notes}\n%s\\\\\n\n", notes_tex)) {
            free(notes_tex);
            return 0;
        }
        free(notes_tex);

        if (device->deficiencies.count > 0) {
            if (!buffer_append_cstr(buf, "\\paragraph{Documented Deficiencies}\n")) {
                return 0;
            }
            if (!buffer_append_cstr(buf,
                "\\begin{tabularx}{\\textwidth}{@{}p{.18\\textwidth} p{.18\\textwidth} p{.18\\textwidth} X@{}}\\toprule\n"
                "\\textbf{Equipment} & \\textbf{Condition} & \\textbf{Remedy} & \\textbf{Note} \\\\ \\midrule\n")) {
                return 0;
            }
            for (size_t j = 0; j < device->deficiencies.count; ++j) {
                ReportDeficiency *def = &device->deficiencies.items[j];
                const char *equip_src = def->equipment ? def->equipment : "—";
                const char *cond_src = def->condition ? def->condition : "—";
                const char *remedy_src = def->remedy ? def->remedy : "—";
                const char *note_src = def->note ? def->note : "—";

                char *equip_clean = sanitize_ascii(equip_src);
                char *cond_clean = sanitize_ascii(cond_src);
                char *remedy_clean = sanitize_ascii(remedy_src);
                char *note_clean = sanitize_ascii(note_src);

                const char *equip_text = equip_clean ? equip_clean : equip_src;
                const char *cond_text = cond_clean ? cond_clean : cond_src;
                const char *remedy_text = remedy_clean ? remedy_clean : remedy_src;
                const char *note_text = note_clean ? note_clean : note_src;

                char *equip_tex = latex_escape(equip_text);
                char *cond_tex = latex_escape(cond_text);
                char *remedy_tex = latex_escape(remedy_text);
                char *note_tex = latex_escape(note_text);

                free(equip_clean);
                free(cond_clean);
                free(remedy_clean);
                free(note_clean);

                if (!equip_tex || !cond_tex || !remedy_tex || !note_tex) {
                    free(equip_tex); free(cond_tex); free(remedy_tex); free(note_tex);
                    return 0;
                }

                if (!buffer_appendf(buf, "%s & %s & %s & %s \\\\ \n", equip_tex, cond_tex, remedy_tex, note_tex)) {
                    free(equip_tex); free(cond_tex); free(remedy_tex); free(note_tex);
                    return 0;
                }

                free(equip_tex);
                free(cond_tex);
                free(remedy_tex);
                free(note_tex);
            }
            if (!buffer_append_cstr(buf, "\\bottomrule\n\\end{tabularx}\n\n")) {
                return 0;
            }
        } else {
            if (!buffer_append_cstr(buf, "\\paragraph{Documented Deficiencies}\n\\textit{No deficiencies recorded for this device.}\\\n\n")) {
                return 0;
            }
        }
    }
    return 1;
}

static int append_narrative_block(Buffer *buf, const char *content) {
    if (!buf) {
        return 0;
    }

    const char *source = (content && content[0]) ? content : "Narrative unavailable.";
    char *clean = sanitize_ascii(source);
    const char *text = clean ? clean : source;
    char *work = strdup(text);
    if (!work) {
        free(clean);
        return 0;
    }

    bool in_list = false;
    bool ok = true;
    char *line = work;

    while (line && ok) {
        char *next = strchr(line, '\n');
        if (next) {
            *next = '\0';
            next++;
        }

        char *trimmed = trim_copy(line);
        if (!trimmed) {
            ok = false;
            break;
        }

        char *p = trimmed;
        while (*p == ' ' || *p == '\t') {
            p++;
        }

        if (*p == '\0') {
            free(trimmed);
            if (in_list) {
                ok = buffer_append_cstr(buf, "\\end{itemize}\n\n");
                in_list = false;
            } else {
                ok = buffer_append_cstr(buf, "\n");
            }
            line = next;
            continue;
        }

        size_t hash_count = 0;
        while (p[hash_count] == '#') {
            hash_count++;
        }
        if (hash_count > 0 && (p[hash_count] == ' ' || p[hash_count] == '\t')) {
            if (in_list) {
                ok = buffer_append_cstr(buf, "\\end{itemize}\n\n");
                in_list = false;
            }
            if (ok) {
                const char *heading_start = p + hash_count;
                while (*heading_start == ' ' || *heading_start == '\t') {
                    heading_start++;
                }
                if (*heading_start != '\0') {
                    char *escaped = latex_escape(heading_start);
                    if (!escaped) {
                        ok = false;
                    } else {
                        const char *format = (hash_count == 1)
                            ? "\\subsection*{%s}\n\n"
                            : "\\subsubsection*{%s}\n\n";
                        ok = buffer_appendf(buf, format, escaped);
                        free(escaped);
                    }
                }
            }
            free(trimmed);
            if (!ok) {
                break;
            }
            line = next;
            continue;
        }

        bool bullet = false;
        if (*p == '-' || *p == '*') {
            bullet = true;
            p++;
            while (*p == ' ' || *p == '\t') {
                p++;
            }
        } else if ((*p == 'o' || *p == 'O') && (p[1] == ' ' || p[1] == '\t')) {
            bullet = true;
            p += 2;
            while (*p == ' ' || *p == '\t') {
                p++;
            }
        }

        if (bullet) {
            if (!in_list) {
                ok = buffer_append_cstr(buf, "\\begin{itemize}\n");
                in_list = true;
            }
            if (ok) {
                char *escaped = latex_escape(p);
                if (!escaped) {
                    ok = false;
                } else {
                    ok = buffer_appendf(buf, "  \\item %s\n", escaped);
                    free(escaped);
                }
            }
        } else {
            if (in_list) {
                ok = buffer_append_cstr(buf, "\\end{itemize}\n\n");
                in_list = false;
            }
            if (ok) {
                char *escaped = latex_escape(trimmed);
                if (!escaped) {
                    ok = false;
                } else {
                    ok = buffer_appendf(buf, "%s\n\n", escaped);
                    free(escaped);
                }
            }
        }

        free(trimmed);
        line = next;
    }

    if (ok && in_list) {
        ok = buffer_append_cstr(buf, "\\end{itemize}\n\n");
    }

    free(work);
    free(clean);
    return ok;
}

static void narrative_task_execute(NarrativeTask *task) {
    if (!task) {
        return;
    }
    task->success = 0;
    char *response = NULL;
    char *error = NULL;
    if (!generate_grok_completion(task->system_prompt, task->prompt, &response, &error)) {
        task->error = error ? error : strdup("Narrative generation failed");
        return;
    }
    *(task->slot) = response;
    task->success = 1;
}

static void *narrative_thread_main(void *arg) {
    NarrativeTask *task = (NarrativeTask *)arg;
    narrative_task_execute(task);
    return NULL;
}

static int append_narrative_section(Buffer *buf, const char *title, const char *content) {
    if (!buf || !title) {
        return 0;
    }
    char *title_clean = sanitize_ascii(title);
    const char *title_src = title_clean ? title_clean : title;
    char *title_tex = latex_escape(title_src ? title_src : "");
    free(title_clean);
    if (!title_tex) {
        return 0;
    }
    int ok = buffer_appendf(buf, "\\section{%s}\n\n", title_tex);
    free(title_tex);
    if (!ok) {
        return 0;
    }
    return append_narrative_block(buf, content);
}

static int build_report_latex(const ReportData *report,
                              const NarrativeSet *narratives,
                              const ReportJob *job,
                              const char *output_path,
                              char **error_out) {
    if (!report || !narratives || !job || !output_path) {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid report parameters");
        }
        return 0;
    }

    Buffer buf;
    if (!buffer_init(&buf)) {
        if (error_out && !*error_out) {
            *error_out = strdup("Out of memory");
        }
        return 0;
    }

    int success = 0;

    char *address_clean = NULL;
    char *address_tex = NULL;
    char *owner_clean = NULL;
    char *owner_tex = NULL;
    char *contractor_clean = NULL;
    char *contractor_tex = NULL;
    char *city_clean = NULL;
    char *city_tex = NULL;
    char *date_range_tex = NULL;
    char *client_name_env = NULL;
    char *client_name_tex = NULL;
    char *client_address_env = NULL;
    char *client_address_tex = NULL;
    char *contact_name_env = NULL;
    char *contact_name_tex = NULL;
    char *contact_email_env = NULL;
    char *contact_email_tex = NULL;
    char *asset_location_tex = NULL;
    char *cover_address_plain = NULL;

    const char *address_src = (report->summary.building_address && report->summary.building_address[0])
        ? report->summary.building_address
        : (job->address && job->address[0] ? job->address : "Unknown address");
    address_clean = sanitize_ascii(address_src);
    const char *address_text = address_clean ? address_clean : address_src;
    address_tex = latex_escape(address_text);
    if (!address_tex) goto cleanup;

    const char *owner_src = (report->summary.building_owner && report->summary.building_owner[0])
        ? report->summary.building_owner
        : "Unknown owner";
    owner_clean = sanitize_ascii(owner_src);
    const char *owner_text = owner_clean ? owner_clean : owner_src;
    owner_tex = latex_escape(owner_text);
    if (!owner_tex) goto cleanup;

    const char *contractor_src = (report->summary.elevator_contractor && report->summary.elevator_contractor[0])
        ? report->summary.elevator_contractor
        : "Not specified";
    contractor_clean = sanitize_ascii(contractor_src);
    const char *contractor_text = contractor_clean ? contractor_clean : contractor_src;
    contractor_tex = latex_escape(contractor_text);
    if (!contractor_tex) goto cleanup;

    const char *city_src = (report->summary.city_id && report->summary.city_id[0])
        ? report->summary.city_id
        : "—";
    city_clean = sanitize_ascii(city_src);
    const char *city_text = city_clean ? city_clean : city_src;
    city_tex = latex_escape(city_text);
    if (!city_tex) goto cleanup;

    char date_range_buf[256];
    if (report->summary.audit_range.start && report->summary.audit_range.end) {
        snprintf(date_range_buf, sizeof(date_range_buf), "%s to %s",
                 report->summary.audit_range.start,
                 report->summary.audit_range.end);
    } else if (report->summary.audit_range.start) {
        snprintf(date_range_buf, sizeof(date_range_buf), "Since %s", report->summary.audit_range.start);
    } else if (report->summary.audit_range.end) {
        snprintf(date_range_buf, sizeof(date_range_buf), "Through %s", report->summary.audit_range.end);
    } else {
        snprintf(date_range_buf, sizeof(date_range_buf), "—");
    }
    date_range_tex = latex_escape(date_range_buf);
    if (!date_range_tex) goto cleanup;

    if ((job->cover_street && job->cover_street[0]) ||
        (job->cover_city && job->cover_city[0]) ||
        (job->cover_state && job->cover_state[0]) ||
        (job->cover_zip && job->cover_zip[0])) {
        Buffer cover_buf;
        if (!buffer_init(&cover_buf)) goto cleanup;
        int cover_ok = 1;

        if (job->cover_street && job->cover_street[0]) {
            char *street_clean = sanitize_ascii(job->cover_street);
            const char *street_text = street_clean ? street_clean : job->cover_street;
            if (!buffer_appendf(&cover_buf, "%s", street_text)) {
                cover_ok = 0;
            }
            free(street_clean);
        }

        bool have_city = job->cover_city && job->cover_city[0];
        bool have_state = job->cover_state && job->cover_state[0];
        bool have_zip = job->cover_zip && job->cover_zip[0];
        if (cover_ok && (have_city || have_state || have_zip)) {
            if (cover_buf.length > 0) {
                if (!buffer_append_char(&cover_buf, '\n')) {
                    cover_ok = 0;
                }
            }
            bool wrote_any = false;
            if (cover_ok && have_city) {
                char *city_clean = sanitize_ascii(job->cover_city);
                const char *city_text = city_clean ? city_clean : job->cover_city;
                if (!buffer_appendf(&cover_buf, "%s", city_text)) {
                    cover_ok = 0;
                }
                free(city_clean);
                wrote_any = cover_ok ? true : wrote_any;
            }
            if (cover_ok && have_state) {
                if (wrote_any) {
                    if (!buffer_append_cstr(&cover_buf, ", ")) {
                        cover_ok = 0;
                    }
                }
                if (cover_ok) {
                    char *state_clean = sanitize_ascii(job->cover_state);
                    const char *state_text = state_clean ? state_clean : job->cover_state;
                    if (!buffer_appendf(&cover_buf, "%s", state_text)) {
                        cover_ok = 0;
                    }
                    free(state_clean);
                }
                wrote_any = cover_ok ? true : wrote_any;
            }
            if (cover_ok && have_zip) {
                if (wrote_any) {
                    if (!buffer_append_char(&cover_buf, ' ')) {
                        cover_ok = 0;
                    }
                }
                if (cover_ok) {
                    char *zip_clean = sanitize_ascii(job->cover_zip);
                    const char *zip_text = zip_clean ? zip_clean : job->cover_zip;
                    if (!buffer_appendf(&cover_buf, "%s", zip_text)) {
                        cover_ok = 0;
                    }
                    free(zip_clean);
                }
            }
        }

        if (!cover_ok) {
            buffer_free(&cover_buf);
            goto cleanup;
        }
        cover_address_plain = cover_buf.data;
        cover_buf.data = NULL;
        buffer_free(&cover_buf);
    }

    client_name_env = trim_copy(getenv("REPORT_CLIENT_NAME"));
    const char *client_name_src = NULL;
    if (job->cover_building_owner && job->cover_building_owner[0]) {
        client_name_src = job->cover_building_owner;
    } else if (client_name_env && client_name_env[0]) {
        client_name_src = client_name_env;
    } else {
        client_name_src = owner_text;
    }
    char *client_name_clean = sanitize_ascii(client_name_src);
    const char *client_name_text = client_name_clean ? client_name_clean : client_name_src;
    client_name_tex = latex_escape(client_name_text);
    free(client_name_clean);
    if (!client_name_tex) goto cleanup;

    client_address_env = trim_copy(getenv("REPORT_CLIENT_ADDRESS"));
    const char *client_address_src = NULL;
    if (cover_address_plain && cover_address_plain[0]) {
        client_address_src = cover_address_plain;
    } else if (client_address_env && client_address_env[0]) {
        client_address_src = client_address_env;
    } else {
        client_address_src = address_text;
    }
    char *client_address_clean = sanitize_ascii(client_address_src);
    const char *client_address_text = client_address_clean ? client_address_clean : client_address_src;
    client_address_tex = latex_escape(client_address_text);
    free(client_address_clean);
    if (!client_address_tex) goto cleanup;

    contact_name_env = trim_copy(getenv("REPORT_CONTACT_NAME"));
    const char *default_contact = (contractor_text && contractor_text[0]) ? contractor_text : "Citywide Elevator Consulting";
    const char *contact_name_src = NULL;
    if (job->cover_contact_name && job->cover_contact_name[0]) {
        contact_name_src = job->cover_contact_name;
    } else if (contact_name_env && contact_name_env[0]) {
        contact_name_src = contact_name_env;
    } else {
        contact_name_src = default_contact;
    }
    char *contact_name_clean = sanitize_ascii(contact_name_src);
    const char *contact_name_text = contact_name_clean ? contact_name_clean : contact_name_src;
    contact_name_tex = latex_escape(contact_name_text);
    free(contact_name_clean);
    if (!contact_name_tex) goto cleanup;

    contact_email_env = trim_copy(getenv("REPORT_CONTACT_EMAIL"));
    const char *contact_email_src = NULL;
    if (job->cover_contact_email && job->cover_contact_email[0]) {
        contact_email_src = job->cover_contact_email;
    } else if (contact_email_env && contact_email_env[0]) {
        contact_email_src = contact_email_env;
    } else {
        contact_email_src = "support@citywideportal.io";
    }
    char *contact_email_clean = sanitize_ascii(contact_email_src);
    const char *contact_email_text = contact_email_clean ? contact_email_clean : contact_email_src;
    contact_email_tex = latex_escape(contact_email_text);
    free(contact_email_clean);
    if (!contact_email_tex) goto cleanup;

    const char *asset_location_src = (cover_address_plain && cover_address_plain[0]) ? cover_address_plain : address_text;
    char *asset_location_clean = sanitize_ascii(asset_location_src);
    const char *asset_location_text = asset_location_clean ? asset_location_clean : asset_location_src;
    asset_location_tex = latex_escape(asset_location_text);
    free(asset_location_clean);
    if (!asset_location_tex) goto cleanup;

    if (!buffer_append_cstr(&buf,
        "\\documentclass[12pt]{article}\n"
        "\\usepackage[utf8]{inputenc}\n"
        "\\usepackage[T1]{fontenc}\n"
        "\\usepackage{geometry}\n"
        "\\usepackage{fancyhdr}\n"
        "\\usepackage{graphicx}\n"
        "\\usepackage{datetime}\n"
        "\\usepackage{hyperref}\n"
        "\\usepackage{etoolbox}\n"
        "\\usepackage{array}\n"
        "\\usepackage{helvet}\n"
        "\\usepackage{tabularx}\n"
        "\\usepackage{booktabs}\n"
        "\\usepackage{pgfplots}\n"
        "\\usepackage{tikz}\n"
        "\\usepackage{lmodern}\n"
        "\\usepackage{xcolor}\n"
        "\\usepackage{float}\n"
        "\\geometry{a4paper, left=0.5in, right=0.5in, top=1in, bottom=1in}\n"
        "\\setlength{\\headheight}{26pt}\n"
        "\\pgfplotsset{compat=1.18}\n"
        "\\graphicspath{{./}{./assets/}}\n"
        "\\usepgfplotslibrary{colorbrewer}\n"
        "\\definecolor{tabblue}{RGB}{31,119,180}\n")) goto cleanup;

    if (!buffer_append_cstr(&buf, "\\newcommand{\\clientname}{")) goto cleanup;
    if (!buffer_appendf(&buf, "%s", client_name_tex)) goto cleanup;
    if (!buffer_append_cstr(&buf, "}\n")) goto cleanup;

    if (!buffer_append_cstr(&buf, "\\newcommand{\\clientaddress}{")) goto cleanup;
    if (!buffer_appendf(&buf, "%s", client_address_tex)) goto cleanup;
    if (!buffer_append_cstr(&buf, "}\n")) goto cleanup;

    if (!buffer_append_cstr(&buf, "\\newcommand{\\contactname}{")) goto cleanup;
    if (!buffer_appendf(&buf, "%s", contact_name_tex)) goto cleanup;
    if (!buffer_append_cstr(&buf, "}\n")) goto cleanup;

    if (!buffer_append_cstr(&buf, "\\newcommand{\\contactemail}{")) goto cleanup;
    if (!buffer_appendf(&buf, "%s", contact_email_tex)) goto cleanup;
    if (!buffer_append_cstr(&buf, "}\n")) goto cleanup;

    if (!buffer_append_cstr(&buf, "\\newcommand{\\assetlocation}{")) goto cleanup;
    if (!buffer_appendf(&buf, "%s", asset_location_tex)) goto cleanup;
    if (!buffer_append_cstr(&buf, "}\n")) goto cleanup;

    if (!buffer_append_cstr(&buf,
        "\\pagestyle{empty}\n"
        "\\setlength{\\parskip}{0.5\\baselineskip}\n"
        "\\setlength{\\parindent}{0pt}\n"
        "\\newcommand{\\coverpage}{%\n"
        "    \\newpage\n"
        "    \\vspace*{1cm}%\n"
        "    \\noindent\\includegraphics[width=0.3\\textwidth]{citywide.png}\\par\\vspace{0.5cm}\n"
        "    \\noindent\\textbf{Citywide Elevator Consulting}\\par\n"
        "    991 US HWY 22\\par\n"
        "    Suite 100A\\par\n"
        "    Bridgewater, NJ 08807\\par\\vspace{0.5cm}\n"
        "    \\noindent\\textbf{Client}\\par\n"
        "    \\clientname\\par\n"
        "    \\clientaddress\\par\\vspace{0.25cm}\n"
        "    \\noindent\\textbf{Contact}\\par\n"
        "    \\contactname\\par\n"
        "    email: \\contactemail\\par\\vspace{0.25cm}\n"
        "    \\noindent\\textbf{Asset Location}\\par\n"
        "    \\assetlocation\\par\\vspace{0.5cm}\n"
        "    \\noindent\\textbf{Creation Date:} \\today\\par\\vspace{0.5cm}\n"
        "    \\vfill\n"
        "}\n"
        "\\fancypagestyle{mainstyle}{%\n"
        "    \\fancyhf{}%\n"
        "    \\fancyhead[R]{\\includegraphics[width=0.0375\\textwidth]{square.png}}%\n"
        "    \\fancyfoot[L]{%\n"
        "        \\scriptsize\n"
        "        \\clientname\\\\\n"
        "        \\clientaddress\n"
        "    }%\n"
        "    \\fancyfoot[R]{\\thepage}%\n"
        "    \\renewcommand{\\headrulewidth}{0pt}%\n"
        "    \\renewcommand{\\footrulewidth}{0pt}%\n"
        "    \\setlength{\\headsep}{0.4in}%\n"
        "}\n"
        "\\AtBeginDocument{%\n"
        "    \\normalsize\n"
        "    \\thispagestyle{empty}%\n"
        "    \\coverpage\n"
        "    \\newpage\n"
        "    \\pagestyle{mainstyle}%\n"
        "    \\pagenumbering{arabic}%\n"
        "    \\hypersetup{pdfborder = {0 0 0}}%\n"
        "    \\tableofcontents\n"
        "    \\newpage\n"
        "}\n"
        "\\AtEndDocument{}\n"
        "\\begin{document}\n\n")) goto cleanup;

    if (!buffer_append_cstr(&buf, "\\section{Executive Summary}\n\n")) goto cleanup;
    if (!buffer_append_cstr(&buf, "\\subsection{Overview}\n")) goto cleanup;
    if (!append_narrative_block(&buf, narratives->executive_summary)) goto cleanup;
    if (!buffer_append_cstr(&buf, "\\subsection{Key Findings}\n")) goto cleanup;
    if (!append_narrative_block(&buf, narratives->key_findings)) goto cleanup;

    if (!buffer_append_cstr(&buf, "\\section{Scope of Work}\n\n")) goto cleanup;
    if (!buffer_append_cstr(&buf, "\\subsection{Methodology}\n")) goto cleanup;
    if (!append_narrative_block(&buf, narratives->methodology)) goto cleanup;
    if (!buffer_append_cstr(&buf,
        "\\subsection{Audit Process}\n"
        "The audit process involved a comprehensive evaluation of all elevator equipment, including mechanical components, electrical systems, safety devices, and maintenance records. Each device was inspected according to applicable codes and industry standards.\n\n")) goto cleanup;

    if (!buffer_append_cstr(&buf,
        "\\section{Equipment Summary}\n\n"
        "\\subsection{General Equipment Condition}\n"
        "The following analysis provides an overview of the equipment condition across all devices inspected.\n\n")) goto cleanup;

    if (!buffer_append_cstr(&buf, "\\begin{center}\n\\begin{tabular}{ll}\n\\toprule\n\\textbf{Metric} & \\textbf{Value} \\\\ \\midrule\n")) goto cleanup;

    char number_buf[32];
    if (!buffer_appendf(&buf, "Address & %s \\\\ \n", address_tex)) goto cleanup;
    if (!buffer_appendf(&buf, "Owner & %s \\\\ \n", owner_tex)) goto cleanup;
    if (!buffer_appendf(&buf, "Elevator Contractor & %s \\\\ \n", contractor_tex)) goto cleanup;
    if (!buffer_appendf(&buf, "City ID & %s \\\\ \n", city_tex)) goto cleanup;
    if (!buffer_appendf(&buf, "Audit Date Range & %s \\\\ \n", date_range_tex)) goto cleanup;
    snprintf(number_buf, sizeof(number_buf), "%d", report->summary.total_devices);
    if (!buffer_appendf(&buf, "Total Devices & %s \\\\ \n", number_buf)) goto cleanup;
    snprintf(number_buf, sizeof(number_buf), "%d", report->summary.audit_count);
    if (!buffer_appendf(&buf, "Audit Count & %s \\\\ \n", number_buf)) goto cleanup;
    snprintf(number_buf, sizeof(number_buf), "%d", report->summary.total_deficiencies);
    if (!buffer_appendf(&buf, "Total Deficiencies & %s \\\\ \n", number_buf)) goto cleanup;
    snprintf(number_buf, sizeof(number_buf), "%.2f", report->summary.average_deficiencies_per_device);
    if (!buffer_appendf(&buf, "Average Deficiencies / Device & %s \\\\ \n", number_buf)) goto cleanup;

    if (!buffer_append_cstr(&buf, "\\bottomrule\n\\end{tabular}\n\\end{center}\n\n")) goto cleanup;
    if (!buffer_append_cstr(&buf, "These metrics summarize all submissions included in this report and frame the analyses that follow.\n\n")) goto cleanup;

    if (!buffer_append_cstr(&buf, "\\subsection{Deficiency Patterns}\n")) goto cleanup;
    if (!append_deficiency_code_chart(&buf, report)) goto cleanup;
    if (!append_deficiencies_per_device_chart(&buf, report)) goto cleanup;
    if (!append_controller_age_chart(&buf, report)) goto cleanup;

    if (!append_device_sections(&buf, report)) goto cleanup;
    if (!buffer_append_cstr(&buf, "\\newpage\n")) goto cleanup;

    if (!append_narrative_section(&buf, "Maintenance Performance", narratives->maintenance_performance)) goto cleanup;
    if (!append_narrative_section(&buf, "Recommendations", narratives->recommendations)) goto cleanup;
    if (!append_narrative_section(&buf, "Conclusion", narratives->conclusion)) goto cleanup;

    if (!buffer_append_cstr(&buf, "\\end{document}\n")) goto cleanup;

    if (write_buffer_to_file(output_path, buf.data, buf.length) != 0) {
        if (error_out && !*error_out) {
            char *msg = malloc(128);
            if (msg) {
                snprintf(msg, 128, "Failed to write %s: %s", output_path, strerror(errno));
                *error_out = msg;
            }
        }
        goto cleanup;
    }

    success = 1;

cleanup:
    free(address_clean);
    free(address_tex);
    free(owner_clean);
    free(owner_tex);
    free(contractor_clean);
    free(contractor_tex);
    free(city_clean);
    free(city_tex);
    free(date_range_tex);
    free(client_name_env);
    free(client_name_tex);
    free(client_address_env);
    free(client_address_tex);
    free(contact_name_env);
    free(contact_name_tex);
    free(contact_email_env);
    free(contact_email_tex);
    free(asset_location_tex);
    free(cover_address_plain);
    buffer_free(&buf);
    if (!success && error_out && !*error_out) {
        *error_out = strdup("Failed to build LaTeX report");
    }
    return success;
}


static void handle_client(int client_fd, void *ctx) {
    PGconn *conn = (PGconn *)ctx;
    char header_buffer[MAX_HEADER_SIZE];
    size_t header_len = 0;
    bool header_complete = false;
    char recv_buffer[READ_BUFFER_SIZE];
    ssize_t nread;

    while (!header_complete) {
        nread = recv(client_fd, recv_buffer, sizeof(recv_buffer), 0);
        if (nread <= 0) {
            return;
        }
        if (header_len + (size_t)nread > sizeof(header_buffer)) {
            char *body = build_error_response("Request headers too large");
            send_http_json(client_fd, 431, "Request Header Fields Too Large", body);
            free(body);
            return;
        }
        memcpy(header_buffer + header_len, recv_buffer, (size_t)nread);
        header_len += (size_t)nread;
        char *end_ptr = NULL;
        end_ptr = strstr(header_buffer, "\r\n\r\n");
        if (end_ptr) {
            header_complete = true;
            char *body_start = end_ptr + 4;
            char saved_body_char = *body_start;
            size_t header_size = (size_t)(end_ptr - header_buffer) + 4;
            size_t leftover = header_len - header_size;
            *body_start = '\0';

            char method[8];
            char path[512];
            if (sscanf(header_buffer, "%7s %511s", method, path) != 2) {
                char *body = build_error_response("Malformed request line");
                send_http_json(client_fd, 400, "Bad Request", body);
                free(body);
                return;
            }

            char *query = strchr(path, '?');
            const char *query_string = NULL;
            if (query) {
                *query = '\0';
                query_string = query + 1;
            }

            char *header_lines = strstr(header_buffer, "\r\n");
            if (header_lines) header_lines += 2;

            const char *api_path = NULL;
            bool is_api_path = false;
            if (g_api_prefix_len > 0) {
                if (strncmp(path, g_api_prefix, g_api_prefix_len) == 0) {
                    char next = path[g_api_prefix_len];
                    if (next == '\0' || next == '/' ) {
                        is_api_path = true;
                        api_path = path + g_api_prefix_len;
                        if (!*api_path) {
                            api_path = "/";
                        }
                    }
                }
            } else {
                if (strcmp(path, "/health") == 0 || strncmp(path, "/audits", 7) == 0) {
                    is_api_path = true;
                    api_path = path;
                } else if (strcmp(path, "/") == 0) {
                    is_api_path = true;
                    api_path = path;
                }
            }

            if (strcmp(method, "OPTIONS") == 0) {
                handle_options_request(client_fd);
                return;
            }

            if (strcmp(method, "GET") == 0) {
                if (is_api_path) {
                    routes_handle_get(client_fd, conn, api_path, query_string);
                } else {
                    serve_static_file(client_fd, path);
                }
                return;
            }

            if (strcmp(method, "PATCH") == 0) {
                if (!is_api_path || !api_path) {
                    char *body = build_error_response("Not Found");
                    send_http_json(client_fd, 404, "Not Found", body);
                    free(body);
                    return;
                }

                char *body_json = NULL;
                long body_len = 0;
                int body_status = 400;
                const char *body_error = NULL;
                if (!read_request_body(client_fd, header_lines, body_start, leftover, saved_body_char,
                                       65536, &body_json, &body_len, &body_status, &body_error)) {
                    char *response = build_error_response(body_error ? body_error : "Invalid request body");
                    const char *status_text = body_status == 411 ? "Length Required" :
                                              (body_status == 500 ? "Internal Server Error" : "Bad Request");
                    send_http_json(client_fd, body_status, status_text, response);
                    free(response);
                    return;
                }

                bool handled = routes_handle_patch(client_fd, conn, api_path, body_json);
                free(body_json);
                if (!handled) {
                    char *body = build_error_response("Not Found");
                    send_http_json(client_fd, 404, "Not Found", body);
                    free(body);
                }
                return;
            }

            if (strcmp(method, "POST") == 0 && is_api_path && api_path && strcmp(api_path, "/reports") == 0) {
                char *body_json = NULL;
                long body_len = 0;
                int body_status = 400;
                const char *body_error = NULL;
                if (!read_request_body(client_fd, header_lines, body_start, leftover, saved_body_char,
                                       262144, &body_json, &body_len, &body_status, &body_error)) {
                    char *response = build_error_response(body_error ? body_error : "Invalid JSON payload");
                    const char *status_text = body_status == 411 ? "Length Required" :
                                              (body_status == 500 ? "Internal Server Error" : "Bad Request");
                    send_http_json(client_fd, body_status, status_text, response);
                    free(response);
                    return;
                }
                log_info("/reports content-length=%ld leftover=%zu", body_len, leftover);

                char *parse_error = NULL;
                JsonValue *root = json_parse(body_json, &parse_error);
                if (!root || root->type != JSON_OBJECT) {
                    const char *reason = parse_error ? parse_error : "parser returned non-object";
                    log_error("/reports payload parse failure: %s", reason);
                    if (body_json) {
                        log_error("/reports raw payload: %.*s", (int)body_len, body_json);
                    }
                    free(body_json);
                    char *body = build_error_response("Invalid JSON payload");
                    send_http_json(client_fd, 400, "Bad Request", body);
                    free(body);
                    if (root) json_free(root);
                    free(parse_error);
                    return;
                }

                JsonValue *addr_val = json_object_get(root, "address");
                const char *addr_raw = json_as_string(addr_val);
                char *address_value = addr_raw ? trim_copy(addr_raw) : NULL;
                if (!address_value || address_value[0] == '\0') {
                    json_free(root);
                    free(parse_error);
                    free(body_json);
                    free(address_value);
                    char *body = build_error_response("address field is required");
                    send_http_json(client_fd, 400, "Bad Request", body);
                    free(body);
                    return;
                }

                JsonValue *notes_val = json_object_get(root, "notes");
                const char *notes_raw = json_as_string(notes_val);
                char *notes_value = NULL;
                if (notes_raw) {
                    notes_value = trim_copy(notes_raw);
                    if (notes_value && notes_value[0] == '\0') {
                        free(notes_value);
                        notes_value = NULL;
                    }
                }

                JsonValue *recs_val = json_object_get(root, "recommendations");
                const char *recs_raw = json_as_string(recs_val);
                char *recs_value = NULL;
                if (recs_raw) {
                    recs_value = trim_copy(recs_raw);
                    if (recs_value && recs_value[0] == '\0') {
                        free(recs_value);
                        recs_value = NULL;
                    }
                }

                char *cover_owner_value = NULL;
                char *cover_street_value = NULL;
                char *cover_city_value = NULL;
                char *cover_state_value = NULL;
                char *cover_zip_value = NULL;
                char *cover_contact_name_value = NULL;
                char *cover_contact_email_value = NULL;

                const struct {
                    const char *key;
                    char **target;
                } cover_fields[] = {
                    {"cover_building_owner", &cover_owner_value},
                    {"cover_street", &cover_street_value},
                    {"cover_city", &cover_city_value},
                    {"cover_state", &cover_state_value},
                    {"cover_zip", &cover_zip_value},
                    {"cover_contact_name", &cover_contact_name_value},
                    {"cover_contact_email", &cover_contact_email_value}
                };

                for (size_t i = 0; i < sizeof(cover_fields) / sizeof(cover_fields[0]); ++i) {
                    JsonValue *field_val = json_object_get(root, cover_fields[i].key);
                    const char *raw = json_as_string(field_val);
                    if (!raw) {
                        continue;
                    }
                    char *trimmed = trim_copy(raw);
                    if (!trimmed || trimmed[0] == '\0') {
                        free(trimmed);
                        continue;
                    }
                    *cover_fields[i].target = trimmed;
                }

                bool has_cover_overrides =
                    (cover_owner_value && cover_owner_value[0]) ||
                    (cover_street_value && cover_street_value[0]) ||
                    (cover_city_value && cover_city_value[0]) ||
                    (cover_state_value && cover_state_value[0]) ||
                    (cover_zip_value && cover_zip_value[0]) ||
                    (cover_contact_name_value && cover_contact_name_value[0]) ||
                    (cover_contact_email_value && cover_contact_email_value[0]);

                json_free(root);
                free(parse_error);
                free(body_json);

                ReportJob request;
                report_job_init(&request);
                request.address = address_value;
                address_value = NULL;
                request.notes = notes_value;
                notes_value = NULL;
                request.recommendations = recs_value;
                recs_value = NULL;
                request.cover_building_owner = cover_owner_value;
                cover_owner_value = NULL;
                request.cover_street = cover_street_value;
                cover_street_value = NULL;
                request.cover_city = cover_city_value;
                cover_city_value = NULL;
                request.cover_state = cover_state_value;
                cover_state_value = NULL;
                request.cover_zip = cover_zip_value;
                cover_zip_value = NULL;
                request.cover_contact_name = cover_contact_name_value;
                cover_contact_name_value = NULL;
                request.cover_contact_email = cover_contact_email_value;
                cover_contact_email_value = NULL;

                char *existing_job_id = NULL;
                char *existing_status = NULL;
                char *existing_output_path = NULL;
                char *lookup_error = NULL;
                int existing = db_find_existing_report_job(conn, request.address, &existing_job_id, &existing_status, &existing_output_path, &lookup_error);
                if (existing < 0) {
                    char *body = build_error_response(lookup_error ? lookup_error : "Failed to check existing reports");
                    send_http_json(client_fd, 500, "Internal Server Error", body);
                    free(body);
                    free(lookup_error);
                    report_job_clear(&request);
                    return;
                }
                free(lookup_error);

                bool reuse_job = false;
                bool artifact_ready = false;
                if (existing == 1 && existing_status) {
                    if (strcmp(existing_status, "queued") == 0 || strcmp(existing_status, "processing") == 0) {
                        reuse_job = true;
                    } else if (strcmp(existing_status, "completed") == 0 && existing_output_path && access(existing_output_path, R_OK) == 0) {
                        reuse_job = true;
                        artifact_ready = true;
                    }
                }

                if (has_cover_overrides) {
                    reuse_job = false;
                }

                if (reuse_job) {
                    char *download_url = artifact_ready ? build_download_url(existing_job_id) : NULL;
                    int http_status = artifact_ready ? 200 : 202;
                    send_report_job_response(client_fd, http_status,
                                             existing_status ? existing_status : (artifact_ready ? "completed" : "queued"),
                                             existing_job_id,
                                             request.address,
                                             download_url);
                    free(download_url);
                    free(existing_job_id);
                    free(existing_status);
                    free(existing_output_path);
                    report_job_clear(&request);
                    return;
                }

                free(existing_job_id);
                free(existing_status);
                free(existing_output_path);

                char job_id[37];
                if (!generate_uuid_v4(job_id)) {
                    report_job_clear(&request);
                    char *body = build_error_response("Failed to create job id");
                    send_http_json(client_fd, 500, "Internal Server Error", body);
                    free(body);
                    return;
                }

                char *insert_error = NULL;
                if (!db_insert_report_job(conn, job_id, &request, &insert_error)) {
                    char *body = build_error_response(insert_error ? insert_error : "Failed to create report job");
                    send_http_json(client_fd, 500, "Internal Server Error", body);
                    free(body);
                    free(insert_error);
                    report_job_clear(&request);
                    return;
                }
                free(insert_error);

                send_report_job_response(client_fd, 202, "queued", job_id, request.address, NULL);
                report_job_clear(&request);
                signal_report_worker();
                return;
            }

            if (strcmp(method, "POST") != 0) {
                char *body = build_error_response("Method Not Allowed");
                send_http_json(client_fd, 405, "Method Not Allowed", body);
                free(body);
                return;
            }

            // POST is only supported on the ingest endpoint root (e.g. /webhook)
            if (!is_api_path || !(api_path && (strcmp(api_path, "/") == 0))) {
                char *body = build_error_response("Not Found");
                send_http_json(client_fd, 404, "Not Found", body);
                free(body);
                return;
            }

            long content_length = -1;
            bool api_key_validated = false;
            char *line = header_lines;
            while (line && *line) {
                char *next = strstr(line, "\r\n");
                if (!next) break;
                if (next == line) {
                    break; // blank line
                }
                size_t len = (size_t)(next - line);
                if (len >= 15 && strncasecmp(line, "Content-Length:", 15) == 0) {
                    const char *value = line + 15;
                    while (*value == ' ' || *value == '\t') value++;
                    content_length = strtol(value, NULL, 10);
                }
                if (len >= 10 && strncasecmp(line, "X-API-Key:", 10) == 0) {
                    const char *value = line + 10;
                    while (*value == ' ' || *value == '\t') value++;
                    char saved = line[len];
                    line[len] = '\0';
                    char *trimmed = trim_copy(value);
                    line[len] = saved;
                    if (trimmed) {
                        if (g_api_key && strcmp(trimmed, g_api_key) == 0) {
                            api_key_validated = true;
                        }
                        free(trimmed);
                    }
                }
                line = next + 2;
            }
            if (content_length < 0) {
                char *body = build_error_response("Content-Length required");
                send_http_json(client_fd, 411, "Length Required", body);
                free(body);
                return;
            }
            if (!api_key_validated) {
                char *body = build_error_response("Unauthorized");
                send_http_json(client_fd, 401, "Unauthorized", body);
                free(body);
                return;
            }

            StringArray processed;
            string_array_init(&processed);
            char *process_error = NULL;
            int ingest_status = 500;
            if (!handle_zip_upload(client_fd, body_start, leftover, saved_body_char, content_length,
                                   conn, &processed, &ingest_status, &process_error)) {
                const char *status_text;
                switch (ingest_status) {
                    case 200: status_text = "OK"; break;
                    case 400: status_text = "Bad Request"; break;
                    case 401: status_text = "Unauthorized"; break;
                    case 411: status_text = "Length Required"; break;
                    case 413: status_text = "Payload Too Large"; break;
                    case 500: default: status_text = "Internal Server Error"; break;
                }
                char *body = build_error_response(process_error ? process_error : "Processing failed");
                const char *payload = body ? body : "{\"status\":\"error\",\"message\":\"Processing failed\"}";
                send_http_json(client_fd, ingest_status, status_text, payload);
                free(body);
                free(process_error);
                string_array_clear(&processed);
                return;
            }
            free(process_error);
            char *body = build_success_response(&processed);
            if (!body) {
                body = build_error_response("Failed to build response");
                send_http_json(client_fd, 500, "Internal Server Error", body);
                free(body);
            } else {
                send_http_json(client_fd, 200, "OK", body);
                free(body);
            }
            string_array_clear(&processed);
            return;
        }
    }
char *body = build_error_response("Incomplete HTTP request");
send_http_json(client_fd, 400, "Bad Request", body);
    free(body);
}

int main(int argc, char **argv) {
    (void)argc;
    (void)argv;

    signal(SIGPIPE, SIG_IGN);

    int exit_code = 1;
    PGconn *conn = NULL;

    const char *env_file = getenv("ENV_FILE");
    if (!env_file || env_file[0] == '\0') {
        env_file = ".env";
    }
    if (!load_env_file(env_file)) {
        goto cleanup;
    }

    const char *dsn = getenv("DATABASE_URL");
    if (!dsn || dsn[0] == '\0') {
        dsn = getenv("POSTGRES_DSN");
    }
    if (!dsn || dsn[0] == '\0') {
        log_error("DATABASE_URL or POSTGRES_DSN must be set");
        goto cleanup;
    }

    g_database_dsn = strdup(dsn);
    if (!g_database_dsn) {
        log_error("Failed to allocate database DSN");
        goto cleanup;
    }

    char *api_key_trimmed = trim_copy(getenv("API_KEY"));
    if (!api_key_trimmed || api_key_trimmed[0] == '\0') {
        log_error("API_KEY must be set");
        free(api_key_trimmed);
        goto cleanup;
    }
    g_api_key = api_key_trimmed;

    char *api_prefix_trimmed = trim_copy(getenv("API_PREFIX"));
    if (!api_prefix_trimmed || api_prefix_trimmed[0] == '\0') {
        free(api_prefix_trimmed);
        api_prefix_trimmed = strdup("/webhook");
    }
    if (!api_prefix_trimmed) {
        log_error("Failed to allocate API prefix");
        goto cleanup;
    }
    if (api_prefix_trimmed[0] != '/') {
        size_t len = strlen(api_prefix_trimmed);
        char *prefixed = malloc(len + 2);
        if (!prefixed) {
            log_error("Failed to allocate API prefix");
            free(api_prefix_trimmed);
            goto cleanup;
        }
        prefixed[0] = '/';
        memcpy(prefixed + 1, api_prefix_trimmed, len + 1);
        free(api_prefix_trimmed);
        api_prefix_trimmed = prefixed;
    }
    size_t prefix_len = strlen(api_prefix_trimmed);
    while (prefix_len > 1 && api_prefix_trimmed[prefix_len - 1] == '/') {
        api_prefix_trimmed[--prefix_len] = '\0';
    }
    if (strcmp(api_prefix_trimmed, "/") == 0) {
        g_api_prefix = strdup("");
        free(api_prefix_trimmed);
        if (!g_api_prefix) {
            log_error("Failed to allocate API prefix");
            goto cleanup;
        }
        g_api_prefix_len = 0;
    } else {
        g_api_prefix = api_prefix_trimmed;
        g_api_prefix_len = strlen(g_api_prefix);
    }

    char *static_dir_trimmed = trim_copy(getenv("STATIC_DIR"));
    if (!static_dir_trimmed || static_dir_trimmed[0] == '\0') {
        free(static_dir_trimmed);
        static_dir_trimmed = strdup("./static");
    }
    if (!static_dir_trimmed) {
        log_error("Failed to allocate static directory");
        goto cleanup;
    }
    g_static_dir = static_dir_trimmed;

    char *report_dir_trimmed = trim_copy(getenv("REPORT_OUTPUT_DIR"));
    if (!report_dir_trimmed || report_dir_trimmed[0] == '\0') {
        free(report_dir_trimmed);
        report_dir_trimmed = strdup("./reports");
    }
    if (!report_dir_trimmed) {
        log_error("Failed to allocate report output directory");
        goto cleanup;
    }
    if (ensure_directory_exists(report_dir_trimmed) != 0) {
        log_error("Failed to initialize report output directory %s", report_dir_trimmed);
        free(report_dir_trimmed);
        goto cleanup;
    }
    g_report_output_dir = report_dir_trimmed;

    char *assets_dir_trimmed = trim_copy(getenv("REPORT_ASSETS_DIR"));
    if (!assets_dir_trimmed || assets_dir_trimmed[0] == '\0') {
        free(assets_dir_trimmed);
        assets_dir_trimmed = strdup("./assets");
    }
    if (!assets_dir_trimmed) {
        log_error("Failed to allocate report assets directory");
        goto cleanup;
    }
    g_report_assets_dir = assets_dir_trimmed;

    RouteHelpers route_helpers = {
        .build_location_detail = build_location_detail_payload,
        .build_report_json = build_report_json_payload
    };
    routes_register_helpers(&route_helpers);
    routes_set_prefix(g_api_prefix);

    char *xai_key_trimmed = trim_copy(getenv("XAI_API_KEY"));
    if (!xai_key_trimmed || xai_key_trimmed[0] == '\0') {
        log_error("XAI_API_KEY must be set");
        free(xai_key_trimmed);
        goto cleanup;
    }
    g_xai_api_key = xai_key_trimmed;

    if (curl_global_init(CURL_GLOBAL_DEFAULT) != 0) {
        log_error("Failed to initialize HTTP client");
        goto cleanup;
    }
    g_curl_initialized = true;

    conn = PQconnectdb(dsn);
    if (PQstatus(conn) != CONNECTION_OK) {
        log_error("Failed to connect to database: %s", PQerrorMessage(conn));
        goto cleanup;
    }
    log_info("Connected to Postgres");

    if (pthread_create(&g_report_thread, NULL, report_worker_main, NULL) != 0) {
        log_error("Failed to start report worker thread");
        goto cleanup;
    }
    g_report_thread_started = true;

    int port = DEFAULT_PORT;
    const char *port_env = getenv("WEBHOOK_PORT");
    if (port_env && port_env[0] != '\0') {
        int parsed = atoi(port_env);
        if (parsed > 0 && parsed < 65535) {
            port = parsed;
        }
    }

    exit_code = http_server_run(port, handle_client, conn) ? 0 : 1;

cleanup:
    if (conn) {
        PQfinish(conn);
        conn = NULL;
    }
    pthread_mutex_lock(&g_report_mutex);
    g_report_stop = true;
    pthread_cond_broadcast(&g_report_cond);
    pthread_mutex_unlock(&g_report_mutex);
    if (g_report_thread_started) {
        pthread_join(g_report_thread, NULL);
        g_report_thread_started = false;
    }
    free(g_api_key);
    g_api_key = NULL;
    free(g_api_prefix);
    g_api_prefix = NULL;
    free(g_static_dir);
    g_static_dir = NULL;
    free(g_report_output_dir);
    g_report_output_dir = NULL;
    free(g_report_assets_dir);
    g_report_assets_dir = NULL;
    free(g_xai_api_key);
    g_xai_api_key = NULL;
    if (g_curl_initialized) {
        curl_global_cleanup();
        g_curl_initialized = false;
    }
    free(g_database_dsn);
    g_database_dsn = NULL;
    return exit_code;
}
