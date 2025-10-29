#ifndef TEXT_UTILS_H
#define TEXT_UTILS_H

char *sanitize_ascii(const char *text);
char *latex_escape(const char *text);
char *latex_escape_with_markdown(const char *text);

#endif /* TEXT_UTILS_H */
