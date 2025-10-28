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

#include "csv.h"
#include "json.h"

#define DEFAULT_PORT 8080
#define MAX_HEADER_SIZE 65536
#define READ_BUFFER_SIZE 8192
#define TEMP_FILE_TEMPLATE "/tmp/audit_zip_XXXXXX"
#define TEMP_DIR_TEMPLATE  "/tmp/audit_unpack_XXXXXX"
static char *g_api_key = NULL;
static char *g_api_prefix = NULL;
static size_t g_api_prefix_len = 0;
static char *g_static_dir = NULL;

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
    char **values;
    size_t count;
    size_t capacity;
} StringArray;

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
    char **items;
    size_t count;
    size_t capacity;
} AllocationList;

static bool is_valid_uuid(const char *uuid);
static void handle_options_request(int client_fd);
static void handle_get_request(int client_fd, PGconn *conn, const char *path);
static char *db_fetch_audit_list(PGconn *conn, char **error_out);
static char *db_fetch_audit_detail(PGconn *conn, const char *uuid, char **error_out);
static void serve_static_file(int client_fd, const char *path);
static const char *mime_type_for(const char *path);
static bool path_is_safe(const char *path);
static bool audit_exists(PGconn *conn, const char *uuid);
static bool db_update_deficiency_status(PGconn *conn, const char *uuid, long deficiency_id, bool resolved, char **resolved_at_out, char **error_out);
static char *build_deficiency_key(const char *overlay_code, const char *device_id, const char *equipment, const char *condition, const char *remedy, const char *note);

static void log_error(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    fprintf(stderr, "[ERROR] ");
    vfprintf(stderr, fmt, args);
    fprintf(stderr, "\n");
    va_end(args);
}

static void log_info(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    fprintf(stderr, "[INFO] ");
    vfprintf(stderr, fmt, args);
    fprintf(stderr, "\n");
    va_end(args);
}

static void optional_int_clear(OptionalInt *value) { value->has_value = false; value->value = 0; }
static void optional_long_clear(OptionalLong *value) { value->has_value = false; value->value = 0; }
static void optional_double_clear(OptionalDouble *value) { value->has_value = false; value->value = 0.0; }
static void optional_bool_clear(OptionalBool *value) { value->has_value = false; value->value = false; }

static void string_array_init(StringArray *array) {
    array->values = NULL;
    array->count = 0;
    array->capacity = 0;
}

static int string_array_append_copy(StringArray *array, const char *value) {
    if (!value) {
        return 1;
    }
    if (array->count == array->capacity) {
        size_t new_cap = array->capacity == 0 ? 4 : array->capacity * 2;
        char **tmp = realloc(array->values, new_cap * sizeof(char *));
        if (!tmp) {
            return 0;
        }
        array->values = tmp;
        array->capacity = new_cap;
    }
    array->values[array->count] = strdup(value);
    if (!array->values[array->count]) {
        return 0;
    }
    array->count++;
    return 1;
}

static void string_array_clear(StringArray *array) {
    if (!array) {
        return;
    }
    for (size_t i = 0; i < array->count; ++i) {
        free(array->values[i]);
    }
    free(array->values);
    array->values = NULL;
    array->count = 0;
    array->capacity = 0;
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

static char *trim_copy(const char *input) {
    if (!input) return NULL;
    const char *start = input;
    while (*start && isspace((unsigned char)*start)) {
        start++;
    }
    const char *end = input + strlen(input);
    while (end > start && isspace((unsigned char)*(end - 1))) {
        end--;
    }
    size_t len = (size_t)(end - start);
    char *out = malloc(len + 1);
    if (!out) return NULL;
    memcpy(out, start, len);
    out[len] = '\0';
    return out;
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

static void trim_inplace(char *str) {
    if (!str) return;
    size_t len = strlen(str);
    size_t start = 0;
    while (start < len && isspace((unsigned char)str[start])) start++;
    size_t end = len;
    while (end > start && isspace((unsigned char)str[end - 1])) end--;
    if (start > 0) {
        memmove(str, str + start, end - start);
    }
    str[end - start] = '\0';
}

static void strip_quotes_inplace(char *str) {
    trim_inplace(str);
    size_t len = strlen(str);
    if (len >= 2 && ((str[0] == '"' && str[len - 1] == '"') || (str[0] == '\'' && str[len - 1] == '\''))) {
        memmove(str, str + 1, len - 2);
        str[len - 2] = '\0';
    }
}

static int load_env_file(const char *path) {
    if (!path || path[0] == '\0') {
        return 1;
    }
    FILE *fp = fopen(path, "r");
    if (!fp) {
        if (errno == ENOENT) {
            log_info("Env file %s not found, skipping", path);
            return 1;
        }
        log_error("Failed to open env file %s: %s", path, strerror(errno));
        return 0;
    }
    char line[1024];
    int line_number = 0;
    while (fgets(line, sizeof(line), fp)) {
        line_number++;
        char *newline = strpbrk(line, "\r\n");
        if (newline) *newline = '\0';
        trim_inplace(line);
        if (line[0] == '\0' || line[0] == '#') {
            continue;
        }
        char *eq = strchr(line, '=');
        if (!eq) {
            log_info("Ignoring malformed env line %d in %s", line_number, path);
            continue;
        }
        *eq = '\0';
        char *key = line;
        char *value = eq + 1;
        trim_inplace(key);
        trim_inplace(value);
        strip_quotes_inplace(value);
        if (key[0] == '\0') {
            continue;
        }
        if (setenv(key, value, 1) != 0) {
            log_error("Failed to set env %s from %s: %s", key, path, strerror(errno));
            fclose(fp);
            return 0;
        }
    }
    fclose(fp);
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
    if (strcmp(trimmed, "1") == 0 || strcasecmp(trimmed, "true") == 0 || strcasecmp(trimmed, "yes") == 0 || strcasecmp(trimmed, "y") == 0) {
        result.has_value = true;
        result.value = true;
    } else if (strcmp(trimmed, "0") == 0 || strcasecmp(trimmed, "false") == 0 || strcasecmp(trimmed, "no") == 0 || strcasecmp(trimmed, "n") == 0) {
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

static char *json_escape_string(const char *input) {
    if (!input) {
        return strdup("");
    }
    size_t capacity = strlen(input) * 2 + 16;
    char *buffer = malloc(capacity);
    if (!buffer) {
        return NULL;
    }
    size_t pos = 0;
    for (const unsigned char *p = (const unsigned char *)input; *p; ++p) {
        unsigned char c = *p;
        if (pos + 6 >= capacity) {
            capacity *= 2;
            char *tmp = realloc(buffer, capacity);
            if (!tmp) {
                free(buffer);
                return NULL;
            }
            buffer = tmp;
        }
        switch (c) {
            case '"': buffer[pos++] = '\\'; buffer[pos++] = '"'; break;
            case '\\': buffer[pos++] = '\\'; buffer[pos++] = '\\'; break;
            case '\b': buffer[pos++] = '\\'; buffer[pos++] = 'b'; break;
            case '\f': buffer[pos++] = '\\'; buffer[pos++] = 'f'; break;
            case '\n': buffer[pos++] = '\\'; buffer[pos++] = 'n'; break;
            case '\r': buffer[pos++] = '\\'; buffer[pos++] = 'r'; break;
            case '\t': buffer[pos++] = '\\'; buffer[pos++] = 't'; break;
            default:
                if (c < 0x20) {
                    snprintf(buffer + pos, 7, "\\u%04x", c);
                    pos += 6;
                } else {
                    buffer[pos++] = (char)c;
                }
                break;
        }
    }
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
static int unzip_to_temp_dir(const char *zip_path, char **out_dir) {
    *out_dir = NULL;
    char *template = strdup(TEMP_DIR_TEMPLATE);
    if (!template) {
        return 0;
    }
    char *dir_path = mkdtemp(template);
    if (!dir_path) {
        log_error("mkdtemp failed: %s", strerror(errno));
        free(template);
        return 0;
    }
    pid_t pid = fork();
    if (pid < 0) {
        log_error("fork failed: %s", strerror(errno));
        free(template);
        return 0;
    }
    if (pid == 0) {
        execlp("unzip", "unzip", "-qq", zip_path, "-d", dir_path, (char *)NULL);
        _exit(127);
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
        log_error("waitpid failed: %s", strerror(errno));
        free(template);
        return 0;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        log_error("unzip failed with status %d", status);
        free(template);
        return 0;
    }
    *out_dir = template;
    return 1;
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

static int process_zip_file(const char *zip_path, PGconn *conn, StringArray *processed_audits, char **error_out) {
    char *temp_dir = NULL;
    if (!unzip_to_temp_dir(zip_path, &temp_dir)) {
        if (error_out) *error_out = strdup("Failed to extract zip archive");
        return 0;
    }

    char *csv_path = NULL;
    char *json_path = NULL;
    PhotoCollection photos;
    if (!collect_files(temp_dir, &csv_path, &json_path, &photos)) {
        if (error_out && !*error_out) *error_out = strdup("Failed to collect extracted files");
        free(csv_path);
        free(json_path);
        remove_directory_recursive(temp_dir);
        free(temp_dir);
        return 0;
    }
    if (!csv_path || !json_path) {
        if (error_out) *error_out = strdup("CSV or JSON file missing in archive");
        free(csv_path);
        free(json_path);
        photo_collection_clear(&photos);
        remove_directory_recursive(temp_dir);
        free(temp_dir);
        return 0;
    }

    char *csv_text = NULL;
    if (!read_file_to_string(csv_path, &csv_text)) {
        if (error_out) *error_out = strdup("Failed to read CSV file");
        free(csv_path);
        free(json_path);
        photo_collection_clear(&photos);
        remove_directory_recursive(temp_dir);
        free(temp_dir);
        return 0;
    }

    CsvFile csv_file;
    char *csv_error = NULL;
    if (!csv_parse(csv_text, &csv_file, &csv_error)) {
        if (error_out) *error_out = csv_error ? csv_error : strdup("Failed to parse CSV content");
        else free(csv_error);
        free(csv_text);
        free(csv_path);
        free(json_path);
        photo_collection_clear(&photos);
        remove_directory_recursive(temp_dir);
        free(temp_dir);
        return 0;
    }

    char *json_text = NULL;
    if (!read_file_to_string(json_path, &json_text)) {
        if (error_out) *error_out = strdup("Failed to read JSON file");
        csv_free(&csv_file);
        free(csv_text);
        free(csv_path);
        free(json_path);
        photo_collection_clear(&photos);
        remove_directory_recursive(temp_dir);
        free(temp_dir);
        return 0;
    }
    char *json_error = NULL;
    JsonValue *json_root = json_parse(json_text, &json_error);
    if (!json_root) {
        if (error_out) *error_out = json_error ? json_error : strdup("Failed to parse JSON content");
        else free(json_error);
        csv_free(&csv_file);
        free(csv_text);
        free(csv_path);
        free(json_path);
        free(json_text);
        photo_collection_clear(&photos);
        remove_directory_recursive(temp_dir);
        free(temp_dir);
        return 0;
    }

    StringArray photo_order;
    if (!parse_photo_names(json_root, &photo_order)) {
        if (error_out) *error_out = strdup("Failed to parse photo list from JSON");
        json_free(json_root);
        csv_free(&csv_file);
        free(csv_text);
        free(csv_path);
        free(json_path);
        free(json_text);
        photo_collection_clear(&photos);
        remove_directory_recursive(temp_dir);
        free(temp_dir);
        return 0;
    }

    DeficiencyList deficiency_list;
    if (!parse_deficiencies(json_root, &deficiency_list)) {
        if (error_out) *error_out = strdup("Failed to parse deficiencies from JSON");
        string_array_clear(&photo_order);
        json_free(json_root);
        csv_free(&csv_file);
        free(csv_text);
        free(csv_path);
        free(json_path);
        free(json_text);
        photo_collection_clear(&photos);
        remove_directory_recursive(temp_dir);
        free(temp_dir);
        return 0;
    }

    if (csv_file.row_count == 0) {
        if (error_out) *error_out = strdup("CSV file did not contain any audit rows");
        deficiency_list_clear(&deficiency_list);
        string_array_clear(&photo_order);
        json_free(json_root);
        csv_free(&csv_file);
        free(csv_text);
        free(csv_path);
        free(json_path);
        free(json_text);
        photo_collection_clear(&photos);
        remove_directory_recursive(temp_dir);
        free(temp_dir);
        return 0;
    }

    for (size_t i = 0; i < csv_file.row_count; ++i) {
        const CsvRow *row = &csv_file.rows[i];
        AuditRecord record;
        char *record_error = NULL;
        if (!populate_audit_record(&csv_file, row, json_root, &record, &record_error)) {
            if (record_error) {
                if (error_out) *error_out = record_error; else free(record_error);
            } else if (error_out) {
                *error_out = strdup("Failed to populate audit record");
            }
            audit_record_free(&record);
            deficiency_list_clear(&deficiency_list);
            string_array_clear(&photo_order);
            json_free(json_root);
            csv_free(&csv_file);
            free(csv_text);
            free(csv_path);
            free(json_path);
            free(json_text);
            photo_collection_clear(&photos);
            remove_directory_recursive(temp_dir);
            free(temp_dir);
            return 0;
        }

        bool existed_before = audit_exists(conn, record.audit_uuid);
        if (existed_before) {
            log_info("Audit %s already exists; overwriting with new data", record.audit_uuid);
        }

        char *upsert_error = NULL;
        if (!db_upsert_audit(conn, &record, &photos, &photo_order, &deficiency_list, &upsert_error)) {
            if (upsert_error) {
                if (error_out) *error_out = upsert_error; else free(upsert_error);
            } else if (error_out) {
                *error_out = strdup("Database insert failed");
            }
            audit_record_free(&record);
            deficiency_list_clear(&deficiency_list);
            string_array_clear(&photo_order);
            json_free(json_root);
            csv_free(&csv_file);
            free(csv_text);
            free(csv_path);
            free(json_path);
            free(json_text);
            photo_collection_clear(&photos);
            remove_directory_recursive(temp_dir);
            free(temp_dir);
            return 0;
        }

        if (!string_array_append_copy(processed_audits, record.audit_uuid)) {
            if (error_out) *error_out = strdup("Failed recording processed audit id");
            audit_record_free(&record);
            deficiency_list_clear(&deficiency_list);
            string_array_clear(&photo_order);
            json_free(json_root);
            csv_free(&csv_file);
            free(csv_text);
            free(csv_path);
            free(json_path);
            free(json_text);
            photo_collection_clear(&photos);
            remove_directory_recursive(temp_dir);
            free(temp_dir);
            return 0;
        }
        audit_record_free(&record);
    }

    deficiency_list_clear(&deficiency_list);
    string_array_clear(&photo_order);
    json_free(json_root);
    csv_free(&csv_file);
    free(csv_text);
    free(json_text);
    free(csv_path);
    free(json_path);
    photo_collection_clear(&photos);
    remove_directory_recursive(temp_dir);
    free(temp_dir);
    return 1;
}
static char *build_success_response(const StringArray *audits) {
    size_t capacity = 64;
    char *buffer = malloc(capacity);
    if (!buffer) return NULL;
    size_t pos = 0;
    const char *prefix = "{\"status\":\"ok\",\"audits\":[";
    size_t prefix_len = strlen(prefix);
    if (pos + prefix_len + 1 > capacity) {
        capacity = prefix_len + 64;
        char *tmp = realloc(buffer, capacity);
        if (!tmp) { free(buffer); return NULL; }
        buffer = tmp;
    }
    memcpy(buffer + pos, prefix, prefix_len);
    pos += prefix_len;

    if (audits) {
        for (size_t i = 0; i < audits->count; ++i) {
            char *escaped = json_escape_string(audits->values[i]);
            if (!escaped) { free(buffer); return NULL; }
            size_t needed = strlen(escaped) + 4;
            if (pos + needed + 1 > capacity) {
                size_t new_cap = capacity * 2;
                while (pos + needed + 1 > new_cap) new_cap *= 2;
                char *tmp = realloc(buffer, new_cap);
                if (!tmp) { free(escaped); free(buffer); return NULL; }
                buffer = tmp;
                capacity = new_cap;
            }
            buffer[pos++] = '"';
            memcpy(buffer + pos, escaped, strlen(escaped));
            pos += strlen(escaped);
            buffer[pos++] = '"';
            if (i + 1 < audits->count) {
                buffer[pos++] = ',';
            }
            free(escaped);
        }
    }
    if (pos + 3 >= capacity) {
        char *tmp = realloc(buffer, capacity + 8);
        if (!tmp) { free(buffer); return NULL; }
        buffer = tmp;
        capacity += 8;
    }
    buffer[pos++] = ']';
    buffer[pos++] = '}';
    buffer[pos] = '\0';
    return buffer;
}

static char *build_error_response(const char *message) {
    char *escaped = json_escape_string(message ? message : "Unknown error");
    if (!escaped) return NULL;
    size_t needed = strlen(escaped) + 32;
    char *buffer = malloc(needed);
    if (!buffer) {
        free(escaped);
        return NULL;
    }
    snprintf(buffer, needed, "{\"status\":\"error\",\"message\":\"%s\"}", escaped);
    free(escaped);
    return buffer;
}

static void send_http_response(int client_fd, int status_code, const char *status_text, const char *content_type, const void *body, size_t body_len) {
    if (!status_text) status_text = "OK";
    if (!content_type) content_type = "application/json";
    char header[512];
    int header_len = snprintf(header, sizeof(header),
                              "HTTP/1.1 %d %s\r\n"
                              "Content-Type: %s\r\n"
                              "Content-Length: %zu\r\n"
                              "Access-Control-Allow-Origin: *\r\n"
                              "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
                              "Access-Control-Allow-Headers: Content-Type, X-API-Key\r\n"
                              "Connection: close\r\n\r\n",
                              status_code, status_text, content_type, body_len);
    if (header_len < 0) return;
    (void)send(client_fd, header, (size_t)header_len, 0);
    if (body_len > 0 && body) {
        (void)send(client_fd, body, body_len, 0);
    }
}

static void send_http_json(int client_fd, int status_code, const char *status_text, const char *json_body) {
    size_t len = json_body ? strlen(json_body) : 0;
    send_http_response(client_fd, status_code, status_text, "application/json", json_body, len);
}

static bool is_valid_uuid(const char *uuid) {
    if (!uuid) return false;
    size_t len = strlen(uuid);
    if (len != 36) {
        return false;
    }
    for (size_t i = 0; i < len; ++i) {
        char c = uuid[i];
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (c != '-') return false;
        } else if (!isxdigit((unsigned char)c)) {
            return false;
        }
    }
    return true;
}

static void handle_options_request(int client_fd) {
    send_http_response(client_fd, 204, "No Content", "application/json", NULL, 0);
}

static char *db_fetch_audit_list(PGconn *conn, char **error_out) {
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

static char *db_fetch_audit_detail(PGconn *conn, const char *uuid, char **error_out) {
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
        return NULL; // not found
    }
    const char *value = PQgetvalue(res, 0, 0);
    char *json = strdup(value ? value : "{}");
    PQclear(res);
    return json;
}

static bool audit_exists(PGconn *conn, const char *uuid) {
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

static bool db_update_deficiency_status(PGconn *conn, const char *uuid, long deficiency_id, bool resolved, char **resolved_at_out, char **error_out) {
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

static void handle_get_request(int client_fd, PGconn *conn, const char *path) {
    if (strcmp(path, "/") == 0 || strcmp(path, "/health") == 0) {
        send_http_json(client_fd, 200, "OK", "{\"status\":\"ok\"}");
        return;
    }

    if (strcmp(path, "/audits") == 0) {
        char *error = NULL;
        char *json = db_fetch_audit_list(conn, &error);
        if (!json) {
            char *body = build_error_response(error ? error : "Failed to fetch audits");
            send_http_json(client_fd, 500, "Internal Server Error", body);
            free(body);
            free(error);
            return;
        }
        send_http_json(client_fd, 200, "OK", json);
        free(json);
        return;
    }

    if (strncmp(path, "/audits/", 8) == 0) {
        const char *uuid_start = path + 8;
        if (*uuid_start == '\0') {
            char *body = build_error_response("Audit ID required");
            send_http_json(client_fd, 400, "Bad Request", body);
            free(body);
            return;
        }
        if (strchr(uuid_start, '/')) {
            char *body = build_error_response("Unknown resource");
            send_http_json(client_fd, 404, "Not Found", body);
            free(body);
            return;
        }
        if (!is_valid_uuid(uuid_start)) {
            char *body = build_error_response("Invalid audit ID");
            send_http_json(client_fd, 400, "Bad Request", body);
            free(body);
            return;
        }
        char *error = NULL;
        char *json = db_fetch_audit_detail(conn, uuid_start, &error);
        if (!json) {
            if (error) {
                char *body = build_error_response(error);
                send_http_json(client_fd, 500, "Internal Server Error", body);
                free(body);
                free(error);
            } else {
                char *body = build_error_response("Audit not found");
                send_http_json(client_fd, 404, "Not Found", body);
                free(body);
            }
            return;
        }
        send_http_json(client_fd, 200, "OK", json);
        free(json);
        return;
    }

    char *body = build_error_response("Not Found");
    send_http_json(client_fd, 404, "Not Found", body);
    free(body);
}

static const char *mime_type_for(const char *path) {
    const char *ext = strrchr(path, '.');
    if (!ext || ext[1] == '\0') {
        return "text/plain; charset=utf-8";
    }
    ext++;
    if (strcasecmp(ext, "html") == 0) return "text/html; charset=utf-8";
    if (strcasecmp(ext, "css") == 0) return "text/css; charset=utf-8";
    if (strcasecmp(ext, "js") == 0) return "text/javascript; charset=utf-8";
    if (strcasecmp(ext, "mjs") == 0) return "text/javascript; charset=utf-8";
    if (strcasecmp(ext, "json") == 0) return "application/json; charset=utf-8";
    if (strcasecmp(ext, "svg") == 0) return "image/svg+xml";
    if (strcasecmp(ext, "png") == 0) return "image/png";
    if (strcasecmp(ext, "jpg") == 0 || strcasecmp(ext, "jpeg") == 0) return "image/jpeg";
    if (strcasecmp(ext, "webp") == 0) return "image/webp";
    if (strcasecmp(ext, "ico") == 0) return "image/x-icon";
    if (strcasecmp(ext, "txt") == 0) return "text/plain; charset=utf-8";
    if (strcasecmp(ext, "map") == 0) return "application/json; charset=utf-8";
    if (strcasecmp(ext, "woff2") == 0) return "font/woff2";
    return "application/octet-stream";
}

static bool path_is_safe(const char *path) {
    if (!path) return false;
    if (strstr(path, "..")) return false;
    if (strchr(path, '\\')) return false;
    return true;
}

static void serve_static_file(int client_fd, const char *path) {
    if (!g_static_dir) {
        char *body = build_error_response("Static content unavailable");
        send_http_json(client_fd, 404, "Not Found", body);
        free(body);
        return;
    }

    char relative[PATH_MAX];
    const char *requested = path && *path ? path : "/";
    if (!path_is_safe(requested)) {
        char *body = build_error_response("Invalid path");
        send_http_json(client_fd, 400, "Bad Request", body);
        free(body);
        return;
    }

    const char *effective = requested;
    if (effective[0] == '/') {
        effective++;
    }

    if (*effective == '\0') {
        strncpy(relative, "index.html", sizeof(relative));
    } else {
        strncpy(relative, effective, sizeof(relative));
        relative[sizeof(relative) - 1] = '\0';
    }

    char full_path[PATH_MAX];
    int written = snprintf(full_path, sizeof(full_path), "%s/%s", g_static_dir, relative);
    if (written < 0 || (size_t)written >= sizeof(full_path)) {
        char *body = build_error_response("Path too long");
        send_http_json(client_fd, 414, "Request-URI Too Long", body);
        free(body);
        return;
    }

    struct stat st;
    bool fallback_to_index = false;
    if (stat(full_path, &st) != 0 || S_ISDIR(st.st_mode)) {
        const bool looks_like_asset = strchr(relative, '.') != NULL;
        if (looks_like_asset) {
            char *body = build_error_response("Not Found");
            send_http_json(client_fd, 404, "Not Found", body);
            free(body);
            return;
        }
        fallback_to_index = true;
    }

    if (fallback_to_index) {
        written = snprintf(full_path, sizeof(full_path), "%s/index.html", g_static_dir);
        if (written < 0 || (size_t)written >= sizeof(full_path) || stat(full_path, &st) != 0) {
            char *body = build_error_response("Static index not found");
            send_http_json(client_fd, 404, "Not Found", body);
            free(body);
            return;
        }
    }

    if (!S_ISREG(st.st_mode)) {
        char *body = build_error_response("Not Found");
        send_http_json(client_fd, 404, "Not Found", body);
        free(body);
        return;
    }

    int fd = open(full_path, O_RDONLY);
    if (fd < 0) {
        char *body = build_error_response("Failed to read static asset");
        send_http_json(client_fd, 500, "Internal Server Error", body);
        free(body);
        return;
    }

    const char *content_type = mime_type_for(full_path);
    send_http_response(client_fd, 200, "OK", content_type, NULL, (size_t)st.st_size);

    off_t offset = 0;
    while (offset < st.st_size) {
        ssize_t sent = sendfile(client_fd, fd, &offset, (size_t)(st.st_size - offset));
        if (sent < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        if (sent == 0) {
            break;
        }
    }

    close(fd);
}
static void handle_client(int client_fd, PGconn *conn) {
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
            size_t header_size = (size_t)(end_ptr - header_buffer) + 4;
            size_t leftover = header_len - header_size;
            header_buffer[header_size] = '\0';

            char method[8];
            char path[512];
            if (sscanf(header_buffer, "%7s %511s", method, path) != 2) {
                char *body = build_error_response("Malformed request line");
                send_http_json(client_fd, 400, "Bad Request", body);
                free(body);
                return;
            }

            char *query = strchr(path, '?');
            if (query) {
                *query = '\0';
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
                    handle_get_request(client_fd, conn, api_path);
                } else {
                    serve_static_file(client_fd, path);
                }
                return;
            }

            if (strcmp(method, "PATCH") == 0) {
                if (!is_api_path || strncmp(api_path, "/audits/", 8) != 0) {
                    char *body = build_error_response("Not Found");
                    send_http_json(client_fd, 404, "Not Found", body);
                    free(body);
                    return;
                }

                const char *rest = api_path + 8;
                const char *slash = strchr(rest, '/');
                if (!slash) {
                    char *body = build_error_response("Invalid deficiency path");
                    send_http_json(client_fd, 400, "Bad Request", body);
                    free(body);
                    return;
                }
                size_t uuid_len = (size_t)(slash - rest);
                if (uuid_len == 0 || uuid_len >= 64) {
                    char *body = build_error_response("Invalid audit id");
                    send_http_json(client_fd, 400, "Bad Request", body);
                    free(body);
                    return;
                }
                char target_uuid[64];
                memcpy(target_uuid, rest, uuid_len);
                target_uuid[uuid_len] = '\0';
                const char *def_path = slash;
                if (strncmp(def_path, "/deficiencies/", 14) != 0) {
                    char *body = build_error_response("Invalid deficiency path");
                    send_http_json(client_fd, 400, "Bad Request", body);
                    free(body);
                    return;
                }
                const char *id_part = def_path + 14;
                if (*id_part == '\0') {
                    char *body = build_error_response("Deficiency id required");
                    send_http_json(client_fd, 400, "Bad Request", body);
                    free(body);
                    return;
                }
                char *endptr = NULL;
                long deficiency_id = strtol(id_part, &endptr, 10);
                if (deficiency_id <= 0 || (endptr && *endptr != '\0')) {
                    char *body = build_error_response("Invalid deficiency id");
                    send_http_json(client_fd, 400, "Bad Request", body);
                    free(body);
                    return;
                }

                long content_length = -1;
                char *line = header_lines;
                while (line && *line) {
                    char *next = strstr(line, "\r\n");
                    if (!next) break;
                    if (next == line) {
                        break;
                    }
                    size_t len = (size_t)(next - line);
                    if (len >= 15 && strncasecmp(line, "Content-Length:", 15) == 0) {
                        const char *value = line + 15;
                        while (*value == ' ' || *value == '\t') value++;
                        content_length = strtol(value, NULL, 10);
                    }
                    line = next + 2;
                }
                if (content_length < 0 || content_length > 65536) {
                    char *body = build_error_response("Invalid request body length");
                    send_http_json(client_fd, 411, "Length Required", body);
                    free(body);
                    return;
                }

                char *body_json = malloc((size_t)content_length + 1);
                if (!body_json) {
                    char *body = build_error_response("Out of memory");
                    send_http_json(client_fd, 500, "Internal Server Error", body);
                    free(body);
                    return;
                }
                size_t offset = 0;
                if (leftover > 0) {
                    size_t copy_len = leftover > (size_t)content_length ? (size_t)content_length : leftover;
                    memcpy(body_json, end_ptr + 4, copy_len);
                    offset += copy_len;
                }
                while ((long)offset < content_length) {
                    ssize_t read_bytes = recv(client_fd, body_json + offset, (size_t)content_length - offset, 0);
                    if (read_bytes <= 0) {
                        free(body_json);
                        char *body = build_error_response("Unexpected end of stream");
                        send_http_json(client_fd, 400, "Bad Request", body);
                        free(body);
                        return;
                    }
                    offset += (size_t)read_bytes;
                }
                body_json[content_length] = '\0';

                char *resolved_field = strstr(body_json, "\"resolved\"");
                bool resolved = false;
                bool resolved_set = false;
                if (resolved_field) {
                    char *colon = strchr(resolved_field, ':');
                    if (colon) {
                        char *ptr = colon + 1;
                        while (*ptr == ' ' || *ptr == '\t' || *ptr == '\r' || *ptr == '\n') ptr++;
                        if (strncmp(ptr, "true", 4) == 0) {
                            resolved = true;
                            resolved_set = true;
                        } else if (strncmp(ptr, "false", 5) == 0) {
                            resolved = false;
                            resolved_set = true;
                        }
                    }
                }
                if (!resolved_set) {
                    free(body_json);
                    char *body = build_error_response("Missing resolved flag");
                    send_http_json(client_fd, 400, "Bad Request", body);
                    free(body);
                    return;
                }

                char *resolved_at = NULL;
                char *update_error = NULL;
                bool success = db_update_deficiency_status(conn, target_uuid, deficiency_id, resolved, &resolved_at, &update_error);
                free(body_json);
                if (!success) {
                    int status = 500;
                    if (update_error && strcmp(update_error, "Deficiency not found") == 0) {
                        status = 404;
                    }
                    char *body = build_error_response(update_error ? update_error : "Update failed");
                    send_http_json(client_fd, status, status == 404 ? "Not Found" : "Internal Server Error", body);
                    free(body);
                    free(update_error);
                    free(resolved_at);
                    return;
                }
                free(update_error);

                char response[256];
                if (resolved_at) {
                    snprintf(response, sizeof(response), "{\"status\":\"ok\",\"resolved\":%s,\"resolved_at\":\"%s\"}", resolved ? "true" : "false", resolved_at);
                } else {
                    snprintf(response, sizeof(response), "{\"status\":\"ok\",\"resolved\":%s,\"resolved_at\":null}", resolved ? "true" : "false");
                }
                free(resolved_at);
                send_http_json(client_fd, 200, "OK", response);
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

            char temp_path[] = TEMP_FILE_TEMPLATE;
            int temp_fd = mkstemp(temp_path);
            if (temp_fd < 0) {
                char *body = build_error_response("Failed to create temporary file");
                send_http_json(client_fd, 500, "Internal Server Error", body);
                free(body);
                return;
            }

            if (leftover > 0) {
                if (!write_all(temp_fd, end_ptr + 4, leftover)) {
                    close(temp_fd);
                    unlink(temp_path);
                    char *body = build_error_response("Failed writing request body");
                    send_http_json(client_fd, 500, "Internal Server Error", body);
                    free(body);
                    return;
                }
            }

            ssize_t total_written = (ssize_t)leftover;
            while (total_written < content_length) {
                nread = recv(client_fd, recv_buffer, sizeof(recv_buffer), 0);
                if (nread <= 0) {
                    close(temp_fd);
                    unlink(temp_path);
                    char *body = build_error_response("Unexpected end of stream");
                    send_http_json(client_fd, 400, "Bad Request", body);
                    free(body);
                    return;
                }
                if (!write_all(temp_fd, recv_buffer, (size_t)nread)) {
                    close(temp_fd);
                    unlink(temp_path);
                    char *body = build_error_response("Failed writing request body");
                    send_http_json(client_fd, 500, "Internal Server Error", body);
                    free(body);
                    return;
                }
                total_written += nread;
            }
            close(temp_fd);

            if (total_written != content_length) {
                unlink(temp_path);
                char *body = build_error_response("Content-Length mismatch");
                send_http_json(client_fd, 400, "Bad Request", body);
                free(body);
                return;
            }

            StringArray processed;
            string_array_init(&processed);
            char *process_error = NULL;
            if (!process_zip_file(temp_path, conn, &processed, &process_error)) {
                char *body = build_error_response(process_error ? process_error : "Processing failed");
                send_http_json(client_fd, 500, "Internal Server Error", body);
                free(body);
                free(process_error);
                string_array_clear(&processed);
                unlink(temp_path);
                return;
            }
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
            unlink(temp_path);
            return;
        }
    }
char *body = build_error_response("Incomplete HTTP request");
send_http_json(client_fd, 400, "Bad Request", body);
    free(body);
}

static int run_server(PGconn *conn, int port) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        log_error("socket failed: %s", strerror(errno));
        return 0;
    }
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        log_error("bind failed: %s", strerror(errno));
        close(server_fd);
        return 0;
    }
    if (listen(server_fd, 16) < 0) {
        log_error("listen failed: %s", strerror(errno));
        close(server_fd);
        return 0;
    }
    log_info("Webhook server listening on port %d", port);

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            log_error("accept failed: %s", strerror(errno));
            break;
        }
        handle_client(client_fd, conn);
        close(client_fd);
    }
    close(server_fd);
    return 1;
}

int main(int argc, char **argv) {
    (void)argc;
    (void)argv;

    signal(SIGPIPE, SIG_IGN);

    const char *env_file = getenv("ENV_FILE");
    if (!env_file || env_file[0] == '\0') {
        env_file = ".env";
    }
    if (!load_env_file(env_file)) {
        return 1;
    }

    const char *dsn = getenv("DATABASE_URL");
    if (!dsn || dsn[0] == '\0') {
        dsn = getenv("POSTGRES_DSN");
    }
    if (!dsn || dsn[0] == '\0') {
        log_error("DATABASE_URL or POSTGRES_DSN must be set");
        return 1;
    }

    char *api_key_trimmed = trim_copy(getenv("API_KEY"));
    if (!api_key_trimmed || api_key_trimmed[0] == '\0') {
        log_error("API_KEY must be set");
        free(api_key_trimmed);
        return 1;
    }
    g_api_key = api_key_trimmed;

    char *api_prefix_trimmed = trim_copy(getenv("API_PREFIX"));
    if (!api_prefix_trimmed || api_prefix_trimmed[0] == '\0') {
        free(api_prefix_trimmed);
        api_prefix_trimmed = strdup("/webhook");
    }
    if (!api_prefix_trimmed) {
        log_error("Failed to allocate API prefix");
        return 1;
    }
    if (api_prefix_trimmed[0] != '/') {
        size_t len = strlen(api_prefix_trimmed);
        char *prefixed = malloc(len + 2);
        if (!prefixed) {
            log_error("Failed to allocate API prefix");
            free(api_prefix_trimmed);
            return 1;
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
            return 1;
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
        return 1;
    }
    g_static_dir = static_dir_trimmed;

    PGconn *conn = PQconnectdb(dsn);
    if (PQstatus(conn) != CONNECTION_OK) {
        log_error("Failed to connect to database: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }
    log_info("Connected to Postgres");

    int port = DEFAULT_PORT;
    const char *port_env = getenv("WEBHOOK_PORT");
    if (port_env && port_env[0] != '\0') {
        int parsed = atoi(port_env);
        if (parsed > 0 && parsed < 65535) {
            port = parsed;
        }
    }

    int ok = run_server(conn, port);
    PQfinish(conn);
    free(g_api_key);
    free(g_api_prefix);
    free(g_static_dir);
    return ok ? 0 : 1;
}
