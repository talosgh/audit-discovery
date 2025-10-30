#ifndef SERVICE_ACTIVITY_H
#define SERVICE_ACTIVITY_H

#include <stddef.h>

typedef enum {
    SERVICE_ACTIVITY_PREVENTATIVE,
    SERVICE_ACTIVITY_TESTING_NO_LOAD,
    SERVICE_ACTIVITY_TESTING_FULL_LOAD,
    SERVICE_ACTIVITY_CALLBACK_EMERGENCY,
    SERVICE_ACTIVITY_CALLBACK_EQUIPMENT,
    SERVICE_ACTIVITY_CALLBACK_VANDALISM,
    SERVICE_ACTIVITY_CALLBACK_ENVIRONMENTAL,
    SERVICE_ACTIVITY_CALLBACK_UTILITY,
    SERVICE_ACTIVITY_CALLBACK_FIRE_PANEL,
    SERVICE_ACTIVITY_CALLBACK_OTHER,
    SERVICE_ACTIVITY_REPAIR,
    SERVICE_ACTIVITY_SITE_VISIT,
    SERVICE_ACTIVITY_STANDBY,
    SERVICE_ACTIVITY_UNKNOWN
} ServiceActivityCategory;

typedef struct {
    const char *code;
    const char *label;
    const char *description;
    ServiceActivityCategory category;
} ServiceActivityInfo;

const ServiceActivityInfo *service_activity_lookup(const char *code);
const char *service_activity_category_name(ServiceActivityCategory category);
const char *service_activity_category_short(ServiceActivityCategory category);

#endif /* SERVICE_ACTIVITY_H */

