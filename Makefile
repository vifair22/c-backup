CC      := gcc
CFLAGS  := -std=c11 -Wall -Wextra -Wpedantic -O3 -flto=auto -march=znver3 -mtune=znver3 \
           -fstack-protector-strong \
           -fstack-clash-protection \
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
LDFLAGS := -flto=auto -llz4 -lssl -lcrypto -lacl -lpthread

BUILD   := build
SRC     := src
TESTS   := tests

SRCS    := $(wildcard $(SRC)/*.c)
OBJS    := $(patsubst $(SRC)/%.c, $(BUILD)/%.o, $(SRCS))
VENDOR_OBJS := $(BUILD)/toml.o $(BUILD)/cJSON.o
OBJS    += $(VENDOR_OBJS)
MAIN_OBJ := $(BUILD)/main.o
LIB_OBJS := $(filter-out $(MAIN_OBJ), $(OBJS))

TARGET        := $(BUILD)/backup
TARGET_STATIC := $(BUILD)/backup-static
TARGET_ASAN   := $(BUILD)/backup-asan

FAULT_SRCS  := $(wildcard $(TESTS)/test_*_fault.c)
TEST_SRCS   := $(filter-out $(FAULT_SRCS), $(filter-out $(TESTS)/fault_inject.c, $(wildcard $(TESTS)/*.c)))
TEST_BINS   := $(patsubst $(TESTS)/%.c, $(BUILD)/%, $(TEST_SRCS))
FAULT_BINS  := $(patsubst $(TESTS)/%.c, $(BUILD)/%, $(FAULT_SRCS))

.PHONY: all static asan clean test bench bench-micro bench-phases

all: $(TARGET)

$(BUILD):
	mkdir -p $(BUILD)

$(BUILD)/%.o: $(SRC)/%.c | $(BUILD)
	$(CC) $(CFLAGS) -MMD -MP -c $< -o $@

VENDOR_CFLAGS := -std=c11 -O3 -flto=auto -march=znver3 -I src -I vendor
$(BUILD)/toml.o: vendor/toml.c | $(BUILD)
	$(CC) $(VENDOR_CFLAGS) -MMD -MP -c $< -o $@
$(BUILD)/cJSON.o: vendor/cJSON.c | $(BUILD)
	$(CC) $(VENDOR_CFLAGS) -MMD -MP -c $< -o $@

-include $(OBJS:.o=.d)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

static: $(TARGET_STATIC)
$(TARGET_STATIC): $(OBJS)
	$(CC) $(CFLAGS) $^ -o $@ -Wl,-Bstatic $(LDFLAGS) -Wl,-Bdynamic

# ASAN build: unusual libs (lz4, ssl, acl) linked statically so the binary
# runs on machines that lack those .so files.  libc/libpthread stay dynamic
# (required by ASAN).  libasan itself is embedded via -static-libasan so the
# target machine does not need libasan.so either.
ASAN_CFLAGS  := $(filter-out -O3 -flto=auto,$(CFLAGS)) -O1 -g -fsanitize=address -fno-omit-frame-pointer
ASAN_OBJS    := $(patsubst $(BUILD)/%.o, $(BUILD)/%-asan.o, $(OBJS))

$(BUILD)/%-asan.o: $(SRC)/%.c | $(BUILD)
	$(CC) $(ASAN_CFLAGS) -MMD -MP -c $< -o $@

$(BUILD)/toml-asan.o: vendor/toml.c | $(BUILD)
	$(CC) -std=c11 -O1 -g -I src -I vendor -MMD -MP -c $< -o $@
$(BUILD)/cJSON-asan.o: vendor/cJSON.c | $(BUILD)
	$(CC) -std=c11 -O1 -g -I src -I vendor -MMD -MP -c $< -o $@

asan: $(TARGET_ASAN)
$(TARGET_ASAN): $(ASAN_OBJS)
	$(CC) $(ASAN_CFLAGS) -static-libasan $^ -o $@ \
	    -Wl,-Bstatic -llz4 -lssl -lcrypto -lacl -Wl,-Bdynamic -lpthread

TEST_CFLAGS := -std=c11 -Wall -Wextra -O2 -march=znver3 -Wno-unused-result -I src -I vendor
# Test binaries link lib objects (not main.o) + cmocka
$(BUILD)/%: $(TESTS)/%.c $(LIB_OBJS) | $(BUILD)
	$(CC) $(TEST_CFLAGS) $^ -o $@ $(LDFLAGS) -lcmocka

FAULT_WRAP := -Wl,--wrap=malloc,--wrap=calloc,--wrap=realloc \
              -Wl,--wrap=fread,--wrap=fwrite,--wrap=fseeko,--wrap=fsync,--wrap=fdatasync

$(BUILD)/test_%_fault: $(TESTS)/test_%_fault.c $(TESTS)/fault_inject.c $(LIB_OBJS) | $(BUILD)
	$(CC) $(TEST_CFLAGS) $(FAULT_WRAP) $^ -o $@ $(LDFLAGS) -lcmocka

test: $(TARGET) $(TEST_BINS) $(FAULT_BINS)
	@for t in $(TEST_BINS) $(FAULT_BINS); do echo "=== $$t ==="; $$t; done

# Benchmark binaries (no cmocka)
BENCH     := bench
BENCH_CFLAGS := -std=c11 -Wall -Wextra -O3 -flto=auto -march=znver3 -mtune=znver3 -Wno-unused-result -I src -I vendor

$(BUILD)/bench_micro: $(BENCH)/micro.c $(LIB_OBJS) | $(BUILD)
	$(CC) $(BENCH_CFLAGS) $^ -o $@ $(LDFLAGS)

$(BUILD)/bench_phases: $(BENCH)/phases.c $(LIB_OBJS) | $(BUILD)
	$(CC) $(BENCH_CFLAGS) $^ -o $@ $(LDFLAGS)

bench: $(BUILD)/bench_micro $(BUILD)/bench_phases
	@echo "=== micro ===" && $(BUILD)/bench_micro
	@echo "=== phases ===" && $(BUILD)/bench_phases

bench-micro: $(BUILD)/bench_micro
	$(BUILD)/bench_micro

bench-phases: $(BUILD)/bench_phases
	$(BUILD)/bench_phases

# ASAN test binaries
ASAN_LIB_OBJS := $(filter-out $(BUILD)/main-asan.o, $(ASAN_OBJS))
ASAN_TEST_CFLAGS := -std=c11 -Wall -Wextra -O1 -g -fsanitize=address -fno-omit-frame-pointer -Wno-unused-result -I src -I vendor
ASAN_TEST_SRCS := $(TEST_SRCS)
ASAN_TEST_BINS := $(patsubst $(TESTS)/%.c, $(BUILD)/%-asan, $(ASAN_TEST_SRCS))

$(BUILD)/%-asan: $(TESTS)/%.c $(ASAN_LIB_OBJS) | $(BUILD)
	$(CC) $(ASAN_TEST_CFLAGS) $^ -o $@ -static-libasan \
	    -Wl,-Bstatic -llz4 -lssl -lcrypto -lacl -Wl,-Bdynamic -lpthread -lcmocka

test-asan: $(TARGET) $(TARGET_ASAN) $(ASAN_TEST_BINS)
	@for t in $(ASAN_TEST_BINS); do echo "=== $$t ==="; $$t || exit 1; done

.PHONY: test-asan

clean:
	rm -rf $(BUILD)
