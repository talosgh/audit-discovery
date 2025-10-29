#include "address_validation.h"

#include "buffer.h"
#include "config.h"
#include "json.h"
#include "json_utils.h"
#include "log.h"
#include "util.h"

#include <curl/curl.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#define GOOGLE_ADDRESS_ENDPOINT "https://addressvalidation.googleapis.com/v1:validateAddress"

typedef struct {
    char *data;
    size_t length;
    size_t capacity;
} HttpBuffer;

static void http_buffer_init(HttpBuffer *buf) {
    if (!buf) return;
    buf->data = NULL;
    buf->length = 0;
    buf->capacity = 0;
}

static void http_buffer_free(HttpBuffer *buf) {
    if (!buf) return;
    free(buf->data);
    buf->data = NULL;
    buf->length = 0;
    buf->capacity = 0;
}

static int http_buffer_reserve(HttpBuffer *buf, size_t additional) {
    if (!buf) return 0;
    if (buf->length + additional + 1 <= buf->capacity) {
        return 1;
    }
    size_t new_cap = buf->capacity == 0 ? (additional + 1024) : buf->capacity * 2;
    while (buf->length + additional + 1 > new_cap) {
        new_cap *= 2;
    }
    char *tmp = realloc(buf->data, new_cap);
    if (!tmp) {
        return 0;
    }
    buf->data = tmp;
    buf->capacity = new_cap;
    return 1;
}

static size_t curl_response_write(void *contents, size_t size, size_t nmemb, void *userp) {
    size_t total = size * nmemb;
    HttpBuffer *buf = (HttpBuffer *)userp;
    if (!http_buffer_reserve(buf, total)) {
        return 0;
    }
    memcpy(buf->data + buf->length, contents, total);
    buf->length += total;
    buf->data[buf->length] = '\0';
    return total;
}

void normalized_address_init(NormalizedAddress *result) {
    if (!result) return;
    memset(result, 0, sizeof(*result));
    result->latitude = NAN;
    result->longitude = NAN;
}

void normalized_address_clear(NormalizedAddress *result) {
    if (!result) return;
    free(result->formatted_address);
    free(result->primary_address_line);
    free(result->city);
    free(result->state);
    free(result->postal_code);
    free(result->postal_code_suffix);
    free(result->country);
    free(result->plus_code);
    free(result->place_id);
    normalized_address_init(result);
}

int normalized_address_clone(const NormalizedAddress *src, NormalizedAddress *dest) {
    if (!src || !dest) {
        return 0;
    }
    normalized_address_init(dest);
    if (src->formatted_address) {
        dest->formatted_address = strdup(src->formatted_address);
        if (!dest->formatted_address) goto oom;
    }
    if (src->primary_address_line) {
        dest->primary_address_line = strdup(src->primary_address_line);
        if (!dest->primary_address_line) goto oom;
    }
    if (src->city) {
        dest->city = strdup(src->city);
        if (!dest->city) goto oom;
    }
    if (src->state) {
        dest->state = strdup(src->state);
        if (!dest->state) goto oom;
    }
    if (src->postal_code) {
        dest->postal_code = strdup(src->postal_code);
        if (!dest->postal_code) goto oom;
    }
    if (src->postal_code_suffix) {
        dest->postal_code_suffix = strdup(src->postal_code_suffix);
        if (!dest->postal_code_suffix) goto oom;
    }
    if (src->country) {
        dest->country = strdup(src->country);
        if (!dest->country) goto oom;
    }
    if (src->plus_code) {
        dest->plus_code = strdup(src->plus_code);
        if (!dest->plus_code) goto oom;
    }
    if (src->place_id) {
        dest->place_id = strdup(src->place_id);
        if (!dest->place_id) goto oom;
    }
    dest->has_geocode = src->has_geocode;
    dest->latitude = src->latitude;
    dest->longitude = src->longitude;
    return 1;

oom:
    normalized_address_clear(dest);
    return 0;
}

static char *append_query_with_key(const char *base_url) {
    if (!g_google_api_key || g_google_api_key[0] == '\0') {
        return NULL;
    }
    size_t len = strlen(base_url) + strlen(g_google_api_key) + 6;
    char *url = malloc(len);
    if (!url) {
        return NULL;
    }
    snprintf(url, len, "%s?key=%s", base_url, g_google_api_key);
    return url;
}

static int populate_normalized_from_json(const JsonValue *root, NormalizedAddress *result, char **error_out) {
    if (!root || root->type != JSON_OBJECT) {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid JSON payload from address validation");
        }
        return 0;
    }

    JsonValue *result_obj = json_object_get(root, "result");
    if (!result_obj || result_obj->type != JSON_OBJECT) {
        if (error_out && !*error_out) {
            *error_out = strdup("Address validation response missing result");
        }
        return 0;
    }

    const char *formatted_address = json_as_string(json_object_get(result_obj, "address.formattedAddress"));
    const JsonValue *postal_address = json_object_get_path(result_obj, "address.postalAddress");
    const JsonValue *usps_address = json_object_get_path(result_obj, "uspsData.standardizedAddress");

    const char *primary_line = NULL;
    const char *city = NULL;
    const char *state = NULL;
    const char *postal_code = NULL;
    const char *postal_suffix = NULL;
    const char *country = NULL;

    if (usps_address && usps_address->type == JSON_OBJECT) {
        primary_line = json_as_string(json_object_get(usps_address, "firstAddressLine"));
        city = json_as_string(json_object_get(usps_address, "city"));
        state = json_as_string(json_object_get(usps_address, "state"));
        postal_code = json_as_string(json_object_get(usps_address, "zipCode"));
        const char *city_state_zip = json_as_string(json_object_get(usps_address, "cityStateZipAddressLine"));
        if (postal_code) {
            const char *hyphen = strchr(postal_code, '-');
            if (hyphen) {
                postal_suffix = hyphen + 1;
            }
        }
        if (!postal_suffix && city_state_zip) {
            const char *dash = strchr(city_state_zip, '-');
            if (dash && strlen(dash + 1) >= 4) {
                postal_suffix = dash + 1;
            }
        }
    }

    if ((!primary_line || !city || !state) && postal_address && postal_address->type == JSON_OBJECT) {
        if (!primary_line) {
            JsonValue *lines = json_object_get(postal_address, "addressLines");
            if (lines && lines->type == JSON_ARRAY && lines->value.array.count > 0) {
                primary_line = json_as_string(lines->value.array.items[0]);
            }
        }
        if (!city) {
            city = json_as_string(json_object_get(postal_address, "locality"));
        }
        if (!state) {
            state = json_as_string(json_object_get(postal_address, "administrativeArea"));
        }
        if (!postal_code) {
            postal_code = json_as_string(json_object_get(postal_address, "postalCode"));
        }
        if (!country) {
            country = json_as_string(json_object_get(postal_address, "regionCode"));
        }
    }

    if (!country) {
        country = json_as_string(json_object_get(result_obj, "address.postalAddress.regionCode"));
    }

    if (!primary_line || !city || !state || !postal_code) {
        if (error_out && !*error_out) {
            *error_out = strdup("Address validation did not return complete components");
        }
        return 0;
    }

    if (formatted_address) {
        result->formatted_address = strdup(formatted_address);
        if (!result->formatted_address) goto oom;
    }
    result->primary_address_line = strdup(primary_line);
    if (!result->primary_address_line) goto oom;
    result->city = strdup(city);
    if (!result->city) goto oom;
    result->state = strdup(state);
    if (!result->state) goto oom;
    result->postal_code = strdup(postal_code);
    if (!result->postal_code) goto oom;
    if (postal_suffix) {
        result->postal_code_suffix = strdup(postal_suffix);
        if (!result->postal_code_suffix) goto oom;
    }
    if (country) {
        result->country = strdup(country);
        if (!result->country) goto oom;
    }

        const JsonValue *geocode = json_object_get(result_obj, "geocode");
        if (geocode && geocode->type == JSON_OBJECT) {
            const JsonValue *location = json_object_get(geocode, "location");
            if (location && location->type == JSON_OBJECT) {
                double lat = json_as_double_default(json_object_get(location, "latitude"), NAN);
                double lng = json_as_double_default(json_object_get(location, "longitude"), NAN);
                if (!isnan(lat) && !isnan(lng)) {
                    result->latitude = lat;
                    result->longitude = lng;
                    result->has_geocode = true;
                }
            }
        const char *plus_code = json_as_string(json_object_get_path(geocode, "plusCode.globalCode"));
        if (plus_code) {
            result->plus_code = strdup(plus_code);
            if (!result->plus_code) goto oom;
        }
        const char *place_id = json_as_string(json_object_get(geocode, "placeId"));
        if (place_id) {
            result->place_id = strdup(place_id);
            if (!result->place_id) goto oom;
        }
    }

    return 1;

oom:
    if (error_out && !*error_out) {
        *error_out = strdup("Out of memory storing normalized address");
    }
    return 0;
}

int validate_address_with_google(const char *raw_address, NormalizedAddress *result, char **error_out) {
    if (error_out) {
        *error_out = NULL;
    }
    if (!result) {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid address output pointer");
        }
        return 0;
    }
    if (!raw_address || raw_address[0] == '\0') {
        if (error_out && !*error_out) {
            *error_out = strdup("Address is empty");
        }
        return 0;
    }
    if (!g_google_api_key || g_google_api_key[0] == '\0') {
        if (error_out && !*error_out) {
            *error_out = strdup("GOOGLE_API_KEY not configured");
        }
        return 0;
    }

    CURL *curl = curl_easy_init();
    if (!curl) {
        if (error_out && !*error_out) {
            *error_out = strdup("Failed to initialize HTTP client");
        }
        return 0;
    }

    Buffer body;
    if (!buffer_init(&body)) {
        curl_easy_cleanup(curl);
        if (error_out && !*error_out) {
            *error_out = strdup("Out of memory preparing request");
        }
        return 0;
    }

    const char *region = (g_google_region_code && g_google_region_code[0]) ? g_google_region_code : "US";
    char *escaped_region = json_escape_string(region);
    char *escaped_address = json_escape_string(raw_address);
    if (!escaped_region || !escaped_address) {
        free(escaped_region);
        free(escaped_address);
        buffer_free(&body);
        curl_easy_cleanup(curl);
        if (error_out && !*error_out) {
            *error_out = strdup("Out of memory preparing JSON body");
        }
        return 0;
    }

    buffer_append_cstr(&body, "{");
    buffer_append_cstr(&body, "\"address\":{");
    buffer_append_cstr(&body, "\"regionCode\":\"");
    buffer_append_cstr(&body, escaped_region);
    buffer_append_cstr(&body, "\",\"languageCode\":\"en\",\"addressLines\":[\"");
    buffer_append_cstr(&body, escaped_address);
    buffer_append_cstr(&body, "\"]}");
    buffer_append_cstr(&body, ",\"enableUspsCass\":true");
    buffer_append_cstr(&body, "}");

    free(escaped_region);
    free(escaped_address);

    char *url = append_query_with_key(GOOGLE_ADDRESS_ENDPOINT);
    if (!url) {
        buffer_free(&body);
        curl_easy_cleanup(curl);
        if (error_out && !*error_out) {
            *error_out = strdup("Failed to construct request URL");
        }
        return 0;
    }

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, "Accept: application/json");

    HttpBuffer response;
    http_buffer_init(&response);

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.data);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_response_write);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

    CURLcode res = curl_easy_perform(curl);
    long status_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status_code);

    curl_slist_free_all(headers);
    buffer_free(&body);
    curl_easy_cleanup(curl);
    free(url);

    if (res != CURLE_OK) {
        if (error_out && !*error_out) {
            const char *msg = curl_easy_strerror(res);
            size_t len = msg ? strlen(msg) : 0;
            char *copy = malloc(len + 32);
            if (copy) {
                snprintf(copy, len + 32, "HTTP request failed: %s", msg ? msg : "unknown error");
                *error_out = copy;
            }
        }
        http_buffer_free(&response);
        return 0;
    }

    if (status_code != 200) {
        if (error_out && !*error_out) {
            if (response.length > 0) {
                *error_out = strdup(response.data);
            } else {
                char *msg = malloc(64);
                if (msg) {
                    snprintf(msg, 64, "HTTP status %ld", status_code);
                    *error_out = msg;
                }
            }
        }
        http_buffer_free(&response);
        return 0;
    }

    JsonValue *root = json_parse(response.data ? response.data : "", error_out);
    http_buffer_free(&response);
    if (!root) {
        return 0;
    }

    int ok = populate_normalized_from_json(root, result, error_out);
    json_free(root);
    return ok;
}
