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
    size_t la = strlen(a);
    size_t lb = strlen(b);
    if (la != lb) {
        return 0;
    }
    for (size_t i = 0; i < la; ++i) {
        if (toupper((unsigned char)a[i]) != toupper((unsigned char)b[i])) {
            return 0;
        }
    }
    return 1;
}

const ServiceActivityInfo *service_activity_lookup(const char *code) {
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

