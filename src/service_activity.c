#include "service_activity.h"

#include <ctype.h>
#include <string.h>

typedef struct {
    ServiceActivityInfo info;
} ServiceActivityEntry;

#define ENTRY(code, label, desc, cat) \
    { { (code), (label), (desc), (cat) } }

static const ServiceActivityEntry g_service_activities[] = {
    ENTRY("PM", "Preventative Maintenance",
          "Scheduled routine maintenance intended to prevent failures (lubrication, adjustments, inspections).",
          SERVICE_ACTIVITY_PREVENTATIVE),
    ENTRY("TST", "No-load testing",
          "Code-mandated periodic testing without test weights (e.g., Category 1, fire service).",
          SERVICE_ACTIVITY_TESTING_NO_LOAD),
    ENTRY("TST-FF", "Firefighter service testing",
          "Firefighter service or related functional testing performed without weights (commonly part of mandated compliance).",
          SERVICE_ACTIVITY_TESTING_NO_LOAD),
    ENTRY("TST-FL", "Full-load testing",
          "Code-mandated testing performed with test weights (e.g., Category 5).",
          SERVICE_ACTIVITY_TESTING_FULL_LOAD),
    ENTRY("CB-EMG", "Callback – Entrapment",
          "Emergency callback to release trapped passengers.",
          SERVICE_ACTIVITY_CALLBACK_EMERGENCY),
    ENTRY("CB-EF", "Callback – Equipment Failure",
          "Reactive callback due to failure of maintainable equipment components.",
          SERVICE_ACTIVITY_CALLBACK_EQUIPMENT),
    ENTRY("CB-MU", "Callback – Misuse/Vandalism",
          "Reactive callback caused by improper use or vandalism (debris, forced doors, broken fixtures).",
          SERVICE_ACTIVITY_CALLBACK_VANDALISM),
    ENTRY("CB-KS", "Callback – Keyswitch",
          "Callback triggered by a keyswitch or operating mode left engaged (independent, inspection, fire service) requiring normalization.",
          SERVICE_ACTIVITY_CALLBACK_OTHER),
    ENTRY("CB-ROA", "Callback – Running on Arrival",
          "Callback where the equipment is running normally when the technician arrives (no fault found, monitoring only).",
          SERVICE_ACTIVITY_CALLBACK_OTHER),
    ENTRY("CB-SB", "Callback – Standby Support",
          "Callback requesting technician standby or on-site presence for events, access, or observation without active repair.",
          SERVICE_ACTIVITY_STANDBY),
    ENTRY("CB-TR", "Callback – Tenant Request",
          "Callback to assist a tenant or patron (e.g., retrieve dropped items, special access) not caused by equipment failure.",
          SERVICE_ACTIVITY_CALLBACK_OTHER),
    ENTRY("CB-NG", "Callback – No Issue Found",
          "Callback closed with no trouble found or no work performed after investigation (no-go).",
          SERVICE_ACTIVITY_CALLBACK_OTHER),
    ENTRY("CB-ENV", "Callback – Environmental",
          "Reactive callback caused by exogenous environmental conditions (fire, flood, lightning, etc.).",
          SERVICE_ACTIVITY_CALLBACK_ENVIRONMENTAL),
    ENTRY("CB-UTIL", "Callback – Utility",
          "Reactive callback attributable to utility disruptions (power quality, brown-outs, utility work).",
          SERVICE_ACTIVITY_CALLBACK_UTILITY),
    ENTRY("CB-FP", "Callback – Fire panel",
          "Callback triggered by building fire panel/monitoring integration events.",
          SERVICE_ACTIVITY_CALLBACK_FIRE_PANEL),
    ENTRY("CB-MISC", "Callback – Other",
          "Reactive callback where the root cause is recorded but does not match a defined bucket.",
          SERVICE_ACTIVITY_CALLBACK_OTHER),
    ENTRY("STBY", "Standby Services",
          "Technician standby or on-site support hours requested by the client.",
          SERVICE_ACTIVITY_STANDBY),
    ENTRY("RP", "Repair Services",
          "Heavy repair or component replacement performed by a repair crew (ropes, motors, major components).",
          SERVICE_ACTIVITY_REPAIR),
    ENTRY("RP-NG", "Repair – No Issue Found",
          "Repair dispatch that ultimately required no work (no-go) after inspection or diagnostics.",
          SERVICE_ACTIVITY_REPAIR),
    ENTRY("RS", "Return Service",
          "Follow-up work after an initial visit, typically to complete pending items (materials or approvals).",
          SERVICE_ACTIVITY_REPAIR),
    ENTRY("SV", "Site Visit / Advisory",
          "Observation, consultation, or survey work without hands-on maintenance.",
          SERVICE_ACTIVITY_SITE_VISIT),
    ENTRY("NDE", "Unclassified / No Data",
          "Log entry without sufficient detail to classify (no access, ambiguous notes).",
          SERVICE_ACTIVITY_UNKNOWN)
};

static int service_activity_code_equals(const char *a, const char *b) {
    if (!a || !b) {
        return 0;
    }
    while (*a && isspace((unsigned char)*a)) ++a;
    while (*b && isspace((unsigned char)*b)) ++b;
    while (*a && *b) {
        unsigned char ca = (unsigned char)*a;
        unsigned char cb = (unsigned char)*b;
        if (toupper(ca) != toupper(cb)) {
            return 0;
        }
        ++a;
        ++b;
    }
    while (*a && isspace((unsigned char)*a)) ++a;
    while (*b && isspace((unsigned char)*b)) ++b;
    return *a == '\0' && *b == '\0';
}

static const ServiceActivityInfo *find_activity_entry(const char *code) {
    if (!code || !*code) {
        return NULL;
    }
    for (size_t i = 0; i < sizeof(g_service_activities)/sizeof(g_service_activities[0]); ++i) {
        if (service_activity_code_equals(code, g_service_activities[i].info.code)) {
            return &g_service_activities[i].info;
        }
    }
    return NULL;
}

static void normalize_activity_code(const char *code, char *buffer, size_t buffer_len) {
    if (!buffer || buffer_len == 0) {
        return;
    }
    buffer[0] = '\0';
    if (!code) {
        return;
    }
    size_t dst = 0;
    size_t len = strlen(code);
    size_t start = 0;
    while (start < len && isspace((unsigned char)code[start])) {
        ++start;
    }
    for (size_t i = start; i < len && dst + 1 < buffer_len; ++i) {
        unsigned char ch = (unsigned char)code[i];
        if (ch == 0) {
            break;
        }
        if (isspace(ch)) {
            continue;
        }
        if (isalnum(ch)) {
            buffer[dst++] = (char)toupper(ch);
            continue;
        }
        if (ch == '-' || ch == '_') {
            buffer[dst++] = '-';
            continue;
        }
        /* Stop at punctuation or non-ASCII markers */
        break;
    }
    buffer[dst] = '\0';
}

const ServiceActivityInfo *service_activity_lookup(const char *code) {
    if (!code || !*code) {
        return NULL;
    }
    char normalized[32];
    normalize_activity_code(code, normalized, sizeof(normalized));
    if (normalized[0] == '\0') {
        return NULL;
    }
    const ServiceActivityInfo *direct = find_activity_entry(normalized);
    if (direct) {
        return direct;
    }

    if (strncmp(normalized, "PM", 2) == 0) {
        return find_activity_entry("PM");
    }
    if (strncmp(normalized, "TST-FL", 6) == 0) {
        return find_activity_entry("TST-FL");
    }
    if (strncmp(normalized, "TST", 3) == 0) {
        return find_activity_entry("TST");
    }
    if (strncmp(normalized, "CB-EMG", 6) == 0) {
        return find_activity_entry("CB-EMG");
    }
    if (strncmp(normalized, "CB-EF", 5) == 0) {
        return find_activity_entry("CB-EF");
    }
    if (strncmp(normalized, "CB-ENV", 6) == 0) {
        return find_activity_entry("CB-ENV");
    }
    if (strncmp(normalized, "CB-", 3) == 0) {
        return find_activity_entry("CB-MISC");
    }
    if (strncmp(normalized, "RP", 2) == 0) {
        return find_activity_entry("RP");
    }
    if (strncmp(normalized, "RS", 2) == 0) {
        return find_activity_entry("RS");
    }
    if (strncmp(normalized, "SV", 2) == 0) {
        return find_activity_entry("SV");
    }
    if (strncmp(normalized, "STBY", 4) == 0) {
        return find_activity_entry("STBY");
    }
    if (strncmp(normalized, "NDE", 3) == 0) {
        return find_activity_entry("NDE");
    }
    return find_activity_entry("NDE");
}

const char *service_activity_category_name(ServiceActivityCategory category) {
    switch (category) {
        case SERVICE_ACTIVITY_PREVENTATIVE: return "Preventative Maintenance";
        case SERVICE_ACTIVITY_TESTING_NO_LOAD: return "Testing – No Load";
        case SERVICE_ACTIVITY_TESTING_FULL_LOAD: return "Testing – Full Load";
        case SERVICE_ACTIVITY_CALLBACK_EMERGENCY: return "Callback – Entrapment";
        case SERVICE_ACTIVITY_CALLBACK_EQUIPMENT: return "Callback – Equipment";
        case SERVICE_ACTIVITY_CALLBACK_VANDALISM: return "Callback – Misuse/Vandalism";
        case SERVICE_ACTIVITY_CALLBACK_ENVIRONMENTAL: return "Callback – Environmental";
        case SERVICE_ACTIVITY_CALLBACK_UTILITY: return "Callback – Utility";
        case SERVICE_ACTIVITY_CALLBACK_FIRE_PANEL: return "Callback – Fire Panel";
        case SERVICE_ACTIVITY_CALLBACK_OTHER: return "Callback – Other";
        case SERVICE_ACTIVITY_REPAIR: return "Repair / Modernization";
        case SERVICE_ACTIVITY_SITE_VISIT: return "Site Visit / Advisory";
        case SERVICE_ACTIVITY_STANDBY: return "Standby Support";
        default: return "Unclassified";
    }
}

const char *service_activity_category_short(ServiceActivityCategory category) {
    switch (category) {
        case SERVICE_ACTIVITY_PREVENTATIVE: return "PM";
        case SERVICE_ACTIVITY_TESTING_NO_LOAD: return "Test (no load)";
        case SERVICE_ACTIVITY_TESTING_FULL_LOAD: return "Test (full load)";
        case SERVICE_ACTIVITY_CALLBACK_EMERGENCY: return "Callback – EMG";
        case SERVICE_ACTIVITY_CALLBACK_EQUIPMENT: return "Callback – Equip";
        case SERVICE_ACTIVITY_CALLBACK_VANDALISM: return "Callback – Vandal";
        case SERVICE_ACTIVITY_CALLBACK_ENVIRONMENTAL: return "Callback – Env";
        case SERVICE_ACTIVITY_CALLBACK_UTILITY: return "Callback – Utility";
        case SERVICE_ACTIVITY_CALLBACK_FIRE_PANEL: return "Callback – Fire";
        case SERVICE_ACTIVITY_CALLBACK_OTHER: return "Callback – Other";
        case SERVICE_ACTIVITY_REPAIR: return "Repair";
        case SERVICE_ACTIVITY_SITE_VISIT: return "Site visit";
        case SERVICE_ACTIVITY_STANDBY: return "Standby";
        default: return "Unclassified";
    }
}
