CC ?= gcc
CFLAGS ?= -std=c11 -Wall -Wextra -pedantic -O2 -D_DEFAULT_SOURCE -D_XOPEN_SOURCE=700
CPPFLAGS ?= -I/usr/include/postgresql
LDFLAGS ?= -lpq

SRC := src/main.c src/csv.c src/json.c
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
