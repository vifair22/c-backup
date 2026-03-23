CC      := gcc
CFLAGS  := -std=c11 -Wall -Wextra -Wpedantic -O2 \
           -Wshadow \
           -Wunused \
           -Wunused-function \
           -Wunused-variable \
           -Wunused-parameter \
           -Wunused-result \
           -Wdouble-promotion \
           -Wformat=2 \
           -Wformat-truncation \
           -Wmissing-prototypes \
           -Wstrict-prototypes \
           -Wmissing-declarations \
           -Wcast-align \
           -Wcast-qual \
           -Wnull-dereference \
           -Wconversion \
           -Wsign-conversion \
           -I src -I vendor
LDFLAGS := -llz4 -lssl -lcrypto -lacl -lpthread

BUILD   := build
SRC     := src
TESTS   := tests

SRCS    := $(wildcard $(SRC)/*.c)
OBJS    := $(patsubst $(SRC)/%.c, $(BUILD)/%.o, $(SRCS))
VENDOR_OBJS := $(BUILD)/toml.o
OBJS    += $(VENDOR_OBJS)
MAIN_OBJ := $(BUILD)/main.o
LIB_OBJS := $(filter-out $(MAIN_OBJ), $(OBJS))

TARGET        := $(BUILD)/backup
TARGET_STATIC := $(BUILD)/backup-static

TEST_SRCS := $(wildcard $(TESTS)/*.c)
TEST_BINS := $(patsubst $(TESTS)/%.c, $(BUILD)/%, $(TEST_SRCS))

.PHONY: all static clean test

all: $(TARGET)

$(BUILD):
	mkdir -p $(BUILD)

$(BUILD)/%.o: $(SRC)/%.c | $(BUILD)
	$(CC) $(CFLAGS) -MMD -MP -c $< -o $@

VENDOR_CFLAGS := -std=c11 -O2 -I src -I vendor
$(BUILD)/toml.o: vendor/toml.c | $(BUILD)
	$(CC) $(VENDOR_CFLAGS) -MMD -MP -c $< -o $@

-include $(OBJS:.o=.d)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

static: $(TARGET_STATIC)
$(TARGET_STATIC): $(OBJS)
	$(CC) $(CFLAGS) $^ -o $@ -Wl,-Bstatic $(LDFLAGS) -Wl,-Bdynamic

TEST_CFLAGS := -std=c11 -Wall -Wextra -O2 -Wno-unused-result -I src -I vendor
# Test binaries link lib objects (not main.o) + cmocka
$(BUILD)/%: $(TESTS)/%.c $(LIB_OBJS) | $(BUILD)
	$(CC) $(TEST_CFLAGS) $^ -o $@ $(LDFLAGS) -lcmocka

test: $(TARGET) $(TEST_BINS)
	@for t in $(TEST_BINS); do echo "=== $$t ==="; $$t; done

clean:
	rm -rf $(BUILD)
