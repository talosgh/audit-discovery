#ifndef ADDRESS_VALIDATION_H
#define ADDRESS_VALIDATION_H

#include <stdbool.h>

typedef struct {
    char *formatted_address;
    char *primary_address_line;
    char *city;
    char *state;
    char *postal_code;
    char *postal_code_suffix;
    char *country;
    char *plus_code;
    char *place_id;
    bool has_geocode;
    double latitude;
    double longitude;
} NormalizedAddress;

void normalized_address_init(NormalizedAddress *result);
void normalized_address_clear(NormalizedAddress *result);
int normalized_address_clone(const NormalizedAddress *src, NormalizedAddress *dest);

int validate_address_with_google(const char *raw_address, NormalizedAddress *result, char **error_out);

#endif /* ADDRESS_VALIDATION_H */
