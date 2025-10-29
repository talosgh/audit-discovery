#ifndef TEXT_UTILS_H
#define TEXT_UTILS_H

char *sanitize_ascii(const char *text);
char *latex_escape(const char *text);
char *latex_escape_with_markdown(const char *text);
char *normalize_caps_if_all_upper(const char *text);
void normalize_caps_inplace(char **text);

#endif /* TEXT_UTILS_H */
