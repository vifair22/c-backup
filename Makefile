MAKEFLAGS += -j$(shell nproc)

CC      := gcc

# ---- Version ----------------------------------------------------------
SEMVER     := $(shell cat release_version 2>/dev/null | tr -d '[:space:]')
BUILD_TS   := $(shell date -u '+%Y%m%d.%H%M')
BUILD_TYPE ?= release
VERSION    := $(SEMVER)_$(BUILD_TS).$(BUILD_TYPE)
VERSION_DEF := -DVERSION_STRING='"$(VERSION)"'

# ---- Directory layout -------------------------------------------------
BUILD      := build
BINS       := $(BUILD)/bins
OBJ_DIR    := $(BUILD)/$(BUILD_TYPE)
SRC        := src
TESTS      := tests

# ---- Flags ------------------------------------------------------------
WARN_FLAGS := -Wshadow -Wunused -Wunused-function -Wunused-variable \
              -Wunused-parameter -Wunused-result -Wdouble-promotion \
              -Wformat=2 -Wformat-truncation -Wmissing-prototypes \
              -Wstrict-prototypes -Wmissing-declarations -Wcast-align \
              -Wcast-qual -Wnull-dereference -Wconversion -Wsign-conversion
SRC_INCLUDES := -I src -I src/cli -I src/ops -I src/store -I src/api -I src/common
COMMON_CFLAGS := -std=c11 -Wall -Wextra -Wpedantic $(WARN_FLAGS) \
                 -fstack-protector-strong -fstack-clash-protection \
                 $(SRC_INCLUDES) -I vendor $(VERSION_DEF)

RELEASE_CFLAGS := $(COMMON_CFLAGS) -O3 -flto=auto -march=znver3 -mtune=znver3
STATIC_CFLAGS  := $(COMMON_CFLAGS) -O3 -flto=auto -march=znver3 -mtune=znver3
DEBUG_CFLAGS   := $(COMMON_CFLAGS) -Og -g -march=znver3 -mtune=znver3
ASAN_CFLAGS    := $(COMMON_CFLAGS) -O1 -g -fsanitize=address -fno-omit-frame-pointer -march=znver3 -mtune=znver3

LDFLAGS        := -llz4 -lssl -lcrypto -lacl -lpthread

# Per-type CFLAGS/LDFLAGS selection
ifeq ($(BUILD_TYPE),release)
  TYPE_CFLAGS  := $(RELEASE_CFLAGS)
  TYPE_LDFLAGS := -flto=auto $(LDFLAGS)
  BIN_NAME     := backup
else ifeq ($(BUILD_TYPE),static)
  TYPE_CFLAGS  := $(STATIC_CFLAGS)
  TYPE_LDFLAGS := -flto=auto -Wl,-Bstatic $(LDFLAGS) -Wl,-Bdynamic
  BIN_NAME     := backup-static
else ifeq ($(BUILD_TYPE),debug)
  TYPE_CFLAGS  := $(DEBUG_CFLAGS)
  TYPE_LDFLAGS := $(LDFLAGS)
  BIN_NAME     := backup-debug
else ifeq ($(BUILD_TYPE),asan)
  TYPE_CFLAGS  := $(ASAN_CFLAGS)
  TYPE_LDFLAGS := -static-libasan -Wl,-Bstatic -llz4 -lssl -lcrypto -lacl -Wl,-Bdynamic -lpthread
  BIN_NAME     := backup-asan
endif

TARGET := $(BINS)/$(BIN_NAME)

# ---- Sources and objects ----------------------------------------------
# Sources live under src/{cli,ops,store,api,common}.  Object files are
# flattened into $(OBJ_DIR) by basename; VPATH lets the pattern rule find
# the .c under any subdirectory.
SRCS        := $(shell find $(SRC) -name '*.c')
SRC_DIRS    := $(sort $(dir $(SRCS)))
VPATH       := $(SRC_DIRS)
OBJS        := $(patsubst %.c, $(OBJ_DIR)/%.o, $(notdir $(SRCS)))
VENDOR_OBJS := $(OBJ_DIR)/toml.o $(OBJ_DIR)/cJSON.o
OBJS        += $(VENDOR_OBJS)
MAIN_OBJ    := $(OBJ_DIR)/main.o
LIB_OBJS    := $(filter-out $(MAIN_OBJ), $(OBJS))

# Vendor flags: match optimisation level but drop strict warnings
ifeq ($(BUILD_TYPE),debug)
  VENDOR_CFLAGS := -std=c11 -Og -g $(SRC_INCLUDES) -I vendor $(VERSION_DEF)
else ifeq ($(BUILD_TYPE),asan)
  VENDOR_CFLAGS := -std=c11 -O1 -g $(SRC_INCLUDES) -I vendor $(VERSION_DEF)
else
  VENDOR_CFLAGS := -std=c11 -O3 -flto=auto -march=znver3 $(SRC_INCLUDES) -I vendor $(VERSION_DEF)
endif

# ---- Phony targets ----------------------------------------------------
.PHONY: all static asan debug clean test bench bench-micro bench-phases analyze lint

all:
	$(MAKE) BUILD_TYPE=release _build

static:
	$(MAKE) BUILD_TYPE=static _build

debug:
	$(MAKE) BUILD_TYPE=debug _build

asan:
	$(MAKE) BUILD_TYPE=asan _build

# ---- Internal build target (called via recursive make) ----------------
.PHONY: _build
_build: $(TARGET)

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

$(BINS):
	mkdir -p $(BINS)

$(OBJ_DIR)/%.o: %.c | $(OBJ_DIR)
	$(CC) $(TYPE_CFLAGS) -MMD -MP -c $< -o $@

$(OBJ_DIR)/toml.o: vendor/toml.c | $(OBJ_DIR)
	$(CC) $(VENDOR_CFLAGS) -MMD -MP -c $< -o $@
$(OBJ_DIR)/cJSON.o: vendor/cJSON.c | $(OBJ_DIR)
	$(CC) $(VENDOR_CFLAGS) -MMD -MP -c $< -o $@

-include $(wildcard $(OBJ_DIR)/*.d)

$(TARGET): $(OBJS) | $(BINS)
	$(CC) $(TYPE_CFLAGS) $^ -o $@ $(TYPE_LDFLAGS)

# ---- Tests (link against release lib objects) -------------------------
REL_OBJ_DIR := $(BUILD)/release
REL_LIB_OBJS = $(filter-out $(REL_OBJ_DIR)/main.o, \
                 $(patsubst %.c, $(REL_OBJ_DIR)/%.o, $(notdir $(SRCS))) \
                 $(REL_OBJ_DIR)/toml.o $(REL_OBJ_DIR)/cJSON.o)

TEST_CFLAGS  := -std=c11 -Wall -Wextra -O2 -march=znver3 -Wno-unused-result $(SRC_INCLUDES) -I vendor
TEST_LDFLAGS := -flto=auto $(LDFLAGS) -lcmocka

FAULT_SRCS  := $(wildcard $(TESTS)/test_*_fault.c)
TEST_SRCS   := $(filter-out $(FAULT_SRCS), $(filter-out $(TESTS)/fault_inject.c, $(wildcard $(TESTS)/*.c)))
TEST_BINS   := $(patsubst $(TESTS)/%.c, $(BINS)/%, $(TEST_SRCS))
FAULT_BINS  := $(patsubst $(TESTS)/%.c, $(BINS)/%, $(FAULT_SRCS))

FAULT_WRAP := -Wl,--wrap=malloc,--wrap=calloc,--wrap=realloc \
              -Wl,--wrap=fread,--wrap=fwrite,--wrap=fseeko,--wrap=fsync,--wrap=fdatasync \
              -Wl,--wrap=sync_file_range

# Ensure release objects exist before linking tests
$(BINS)/%: $(TESTS)/%.c $(REL_LIB_OBJS) | $(BINS)
	$(CC) $(TEST_CFLAGS) $^ -o $@ $(TEST_LDFLAGS)

$(BINS)/test_%_fault: $(TESTS)/test_%_fault.c $(TESTS)/fault_inject.c $(REL_LIB_OBJS) | $(BINS)
	$(CC) $(TEST_CFLAGS) $(FAULT_WRAP) $^ -o $@ $(TEST_LDFLAGS)

test: all $(TEST_BINS) $(FAULT_BINS) analyze
	@for t in $(TEST_BINS) $(FAULT_BINS); do echo "=== $$t ==="; $$t; done

# ---- ASAN tests -------------------------------------------------------
ASAN_OBJ_DIR := $(BUILD)/asan
ASAN_LIB_OBJS = $(filter-out $(ASAN_OBJ_DIR)/main.o, \
                  $(patsubst %.c, $(ASAN_OBJ_DIR)/%.o, $(notdir $(SRCS))) \
                  $(ASAN_OBJ_DIR)/toml.o $(ASAN_OBJ_DIR)/cJSON.o)

ASAN_TEST_CFLAGS := -std=c11 -Wall -Wextra -O1 -g -fsanitize=address -fno-omit-frame-pointer -Wno-unused-result $(SRC_INCLUDES) -I vendor
ASAN_TEST_BINS   := $(patsubst $(TESTS)/%.c, $(BINS)/%-asan, $(TEST_SRCS))

$(BINS)/%-asan: $(TESTS)/%.c $(ASAN_LIB_OBJS) | $(BINS)
	$(CC) $(ASAN_TEST_CFLAGS) $^ -o $@ -static-libasan \
	    -Wl,-Bstatic -llz4 -lssl -lcrypto -lacl -Wl,-Bdynamic -lpthread -lcmocka

test-asan: asan $(ASAN_TEST_BINS)
	@for t in $(ASAN_TEST_BINS); do echo "=== $$t ==="; $$t || exit 1; done

.PHONY: test-asan

# ---- Static analysis --------------------------------------------------
ANALYZE_CFLAGS := -std=c11 -Wall -Wextra -O2 -march=znver3 $(SRC_INCLUDES) -I vendor
STACK_LIMIT    := 65536

analyze: | $(BUILD)
	@echo "=== stack-usage ==="
	@mkdir -p $(BUILD)/analyze
	@for f in $(SRCS); do \
	    base=$$(basename $$f .c); \
	    $(CC) $(ANALYZE_CFLAGS) -fstack-usage -c $$f -o $(BUILD)/analyze/$$base.o 2>/dev/null; \
	done
	@fail=0; for su in $(BUILD)/analyze/*.su; do \
	    [ -f "$$su" ] || continue; \
	    awk -v limit=$(STACK_LIMIT) -v file="$$su" \
	        '$$2+0 > limit { printf "STACK OVERFLOW RISK: %s %s (%s bytes, limit %d)\n", file, $$1, $$2, limit; found=1 } \
	         END { exit (found ? 1 : 0) }' "$$su" || fail=1; \
	done; \
	rm -rf $(BUILD)/analyze; \
	if [ $$fail -ne 0 ]; then echo "stack-usage: FAIL"; exit 1; fi
	@echo "stack-usage: OK"
	@echo "=== gcc-fanalyzer ==="
	@for f in $(SRCS); do \
	    $(CC) $(ANALYZE_CFLAGS) -fanalyzer -fsyntax-only $$f 2>&1; \
	done
	@echo "gcc-fanalyzer: OK"
	@echo "=== cppcheck ==="
	@cppcheck --enable=warning,performance,portability --error-exitcode=1 \
	    --suppress=missingIncludeSystem \
	    --suppress=normalCheckLevelMaxBranches \
	    --suppress=toomanyconfigs \
	    --suppress=*:vendor/* \
	    --suppress=oppositeInnerCondition:src/store/object.c \
	    --suppress=intToPointerCast:src/ops/backup.c \
	    -DVERSION_STRING=\"0.0.0\" \
	    --inline-suppr \
	    --quiet $(SRC_INCLUDES) -I vendor $(SRCS)
	@echo "cppcheck: OK"

lint:
	@echo "=== clang-tidy ==="
	@clang-tidy $(SRCS) -- -std=c11 $(SRC_INCLUDES) -I vendor 2>&1 | \
	    grep -E "warning:|error:" || echo "clang-tidy: OK"

# ---- Benchmarks -------------------------------------------------------
BENCH        := bench
BENCH_CFLAGS := -std=c11 -Wall -Wextra -O3 -flto=auto -march=znver3 -mtune=znver3 -Wno-unused-result -I src -I vendor

$(BINS)/bench_micro: $(BENCH)/micro.c $(REL_LIB_OBJS) | $(BINS)
	$(CC) $(BENCH_CFLAGS) $^ -o $@ -flto=auto $(LDFLAGS)

$(BINS)/bench_phases: $(BENCH)/phases.c $(REL_LIB_OBJS) | $(BINS)
	$(CC) $(BENCH_CFLAGS) $^ -o $@ -flto=auto $(LDFLAGS)

$(BINS)/bench_fuse_xfer: $(BENCH)/fuse_xfer.c $(REL_LIB_OBJS) | $(BINS)
	$(CC) $(BENCH_CFLAGS) $^ -o $@ -flto=auto $(LDFLAGS)

bench: $(BINS)/bench_micro $(BINS)/bench_phases
	@echo "=== micro ===" && $(BINS)/bench_micro
	@echo "=== phases ===" && $(BINS)/bench_phases

bench-micro: $(BINS)/bench_micro
	$(BINS)/bench_micro

bench-phases: $(BINS)/bench_phases
	$(BINS)/bench_phases

# ---- Coverage ---------------------------------------------------------
COV_DIR     := $(BUILD)/coverage
COV_CFLAGS  := -std=c11 -Wall -Wextra -O0 -g --coverage -march=znver3 \
               -Wno-unused-result -Wno-format-truncation -I src -I vendor
COV_LDFLAGS := --coverage $(LDFLAGS)
COV_OBJS    := $(patsubst $(SRC)/%.c, $(COV_DIR)/%.o, $(SRCS))
COV_VENDOR  := $(COV_DIR)/toml.o $(COV_DIR)/cJSON.o
COV_OBJS    += $(COV_VENDOR)
COV_MAIN    := $(COV_DIR)/main.o
COV_LIB     := $(filter-out $(COV_MAIN), $(COV_OBJS))
COV_TESTS   := $(patsubst $(TESTS)/%.c, $(COV_DIR)/%, $(TEST_SRCS))

$(COV_DIR):
	mkdir -p $(COV_DIR)

$(COV_DIR)/%.o: $(SRC)/%.c | $(COV_DIR)
	$(CC) $(COV_CFLAGS) -c $< -o $@

$(COV_DIR)/toml.o: vendor/toml.c | $(COV_DIR)
	$(CC) -std=c11 -O0 -g --coverage -I src -I vendor -c $< -o $@
$(COV_DIR)/cJSON.o: vendor/cJSON.c | $(COV_DIR)
	$(CC) -std=c11 -O0 -g --coverage -I src -I vendor -c $< -o $@

$(COV_DIR)/%: $(TESTS)/%.c $(COV_LIB) | $(COV_DIR)
	$(CC) $(COV_CFLAGS) $^ -o $@ $(COV_LDFLAGS) -lcmocka

coverage: $(COV_TESTS)
	@for t in $(COV_TESTS); do $$t >/dev/null 2>&1 || true; done
	@gcovr -r $(SRC) $(COV_DIR) --html-details $(COV_DIR)/coverage.html \
	    --exclude '.*vendor.*' -s
	@echo "Coverage report: $(COV_DIR)/coverage.html"

.PHONY: coverage

# ---- Clean ------------------------------------------------------------
clean:
	rm -rf $(BUILD)
