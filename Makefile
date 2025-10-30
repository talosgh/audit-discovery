CC ?= gcc
CFLAGS ?= -std=c11 -Wall -Wextra -pedantic -O2 -D_DEFAULT_SOURCE -D_XOPEN_SOURCE=700
CPPFLAGS ?= -Iinclude -I/usr/include/postgresql
LDFLAGS ?= -lpq -lpthread -lcurl -lm

SRC := src/main.c \
       src/csv.c \
       src/json.c \
       src/util.c \
       src/log.c \
       src/config.c \
       src/fsutil.c \
       src/http.c \
       src/buffer.c \
       src/json_utils.c \
       src/server.c \
       src/report_jobs.c \
       src/db_helpers.c \
       src/text_utils.c \
       src/narrative.c \
       src/address_validation.c \
       src/routes.c \
       src/service_activity.c
OBJ := $(SRC:.c=.o)
TARGET := audit_webhook

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CC) $(OBJ) -o $@ $(LDFLAGS)

%.o: %.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJ) $(TARGET)

.PHONY: all clean
