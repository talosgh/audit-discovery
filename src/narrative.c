#include "narrative.h"

#include "buffer.h"
#include "config.h"
#include "json.h"
#include "json_utils.h"
#include "text_utils.h"

#include <curl/curl.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define GROK_API_URL "https://api.x.ai/v1/chat/completions"
#define GROK_MODEL "grok-3-mini-latest"

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

int generate_grok_completion(const char *system_prompt, const char *user_prompt, char **response_out, char **error_out) {
    if (!system_prompt || !user_prompt || !response_out) {
        if (error_out && !*error_out) {
            *error_out = strdup("Invalid narrative parameters");
        }
        return 0;
    }
    if (!g_xai_api_key) {
        if (error_out && !*error_out) {
            *error_out = strdup("XAI_API_KEY not configured");
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
            *error_out = strdup("Out of memory");
        }
        return 0;
    }

    char *system_json = json_escape_string(system_prompt);
    char *user_json = json_escape_string(user_prompt);
    if (!system_json || !user_json) {
        free(system_json);
        free(user_json);
        buffer_free(&body);
        curl_easy_cleanup(curl);
        if (error_out && !*error_out) {
            *error_out = strdup("Out of memory");
        }
        return 0;
    }

    buffer_append_cstr(&body, "{");
    buffer_append_cstr(&body, "\"model\":\"");
    buffer_append_cstr(&body, GROK_MODEL);
    buffer_append_cstr(&body, "\",");
    buffer_append_cstr(&body, "\"temperature\":0.1,");
    buffer_append_cstr(&body, "\"max_tokens\":4000,");
    buffer_append_cstr(&body, "\"messages\":[");
    buffer_append_cstr(&body, "{\"role\":\"system\",\"content\":\"");
    buffer_append_cstr(&body, system_json);
    buffer_append_cstr(&body, "\"},");
    buffer_append_cstr(&body, "{\"role\":\"user\",\"content\":\"");
    buffer_append_cstr(&body, user_json);
    buffer_append_cstr(&body, "\"}]}");

    free(system_json);
    free(user_json);

    size_t auth_len = strlen(g_xai_api_key) + strlen("Authorization: Bearer ") + 1;
    char *auth_header = malloc(auth_len);
    if (!auth_header) {
        buffer_free(&body);
        curl_easy_cleanup(curl);
        if (error_out && !*error_out) {
            *error_out = strdup("Out of memory");
        }
        return 0;
    }
    snprintf(auth_header, auth_len, "Authorization: Bearer %s", g_xai_api_key);

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, "Accept: application/json");
    headers = curl_slist_append(headers, auth_header);

    HttpBuffer response;
    http_buffer_init(&response);

    curl_easy_setopt(curl, CURLOPT_URL, GROK_API_URL);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.data);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 120L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_response_write);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

    CURLcode res = curl_easy_perform(curl);
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
        curl_slist_free_all(headers);
        free(auth_header);
        buffer_free(&body);
        curl_easy_cleanup(curl);
        return 0;
    }

    long status_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status_code);

    curl_slist_free_all(headers);
    free(auth_header);
    buffer_free(&body);
    curl_easy_cleanup(curl);

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

    char *parse_error = NULL;
    JsonValue *root = json_parse(response.data ? response.data : "", &parse_error);
    char *content_copy = NULL;
    if (root) {
        JsonValue *choices = json_object_get(root, "choices");
        if (choices && choices->type == JSON_ARRAY && json_array_size(choices) > 0) {
            JsonValue *first = json_array_get(choices, 0);
            JsonValue *message = json_object_get(first, "message");
            if (message && message->type == JSON_OBJECT) {
                JsonValue *content = json_object_get(message, "content");
                const char *text = json_as_string(content);
                if (text) {
                    content_copy = strdup(text);
                }
            }
        }
        json_free(root);
    }
    if (parse_error) {
        free(parse_error);
    }

    http_buffer_free(&response);

    if (!content_copy) {
        if (error_out && !*error_out) {
            *error_out = strdup("Failed to parse Grok response");
        }
        return 0;
    }

    char *sanitized = sanitize_ascii(content_copy);
    if (sanitized) {
        free(content_copy);
        content_copy = sanitized;
    }

    *response_out = content_copy;
    return 1;
}
