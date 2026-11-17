MAKEFLAGS += -j$(shell nproc)

# A bare `make` builds the programs. The per-build-type flags.stamp rules are
# defined before `all`, and without this the first of them became the default
# goal, so `make` (as the Dockerfile runs it) built nothing at all.
.DEFAULT_GOAL := all

CC      := gcc

# ---- c-utils ----------------------------------------------------------
# Sibling checkout by default. cutils_version pins the tag CI builds against;
# a local checkout that has drifted from the pin gets a warning rather than a
# hard failure, so day-to-day work against an unreleased c-utils still builds.
CUTILS_DIR ?= ../c-utils

# ---- Version ----------------------------------------------------------
SEMVER     := $(shell cat release_version 2>/dev/null | tr -d '[:space:]')
BUILD_TS   := $(shell date -u '+%Y%m%d.%H%M')
BUILD_TYPE ?= release
VERSION    := $(SEMVER)_$(BUILD_TS).$(BUILD_TYPE)
VERSION_DEF := -DVERSION_STRING='"$(VERSION)"'

CUTILS_SEMVER      := $(shell cat $(CUTILS_DIR)/release_version 2>/dev/null | tr -d '[:space:]')
CUTILS_VERSION_DEF := -DCUTILS_VERSION_STRING='"$(CUTILS_SEMVER)_$(BUILD_TS).release"'
CUTILS_REF         := $(shell cat cutils_version 2>/dev/null | tr -d '[:space:]')
ifneq ($(CUTILS_SEMVER),)
ifneq ($(CUTILS_REF),)
ifneq (v$(CUTILS_SEMVER),$(CUTILS_REF))
$(warning sibling c-utils $(CUTILS_SEMVER) differs from pinned $(CUTILS_REF))
endif
endif
endif

# ---- OpenSSL floor ----------------------------------------------------
# Argon2id key derivation needs OpenSSL 3.2. Older (Ubuntu 24.04 ships 3.0)
# fails deep in repo_key.c on an undeclared constant that does not say why,
# so a build checks up front. Only when building: clean and the analysers
# need no OpenSSL. Skipped when pkg-config cannot see OpenSSL at all, in which
# case repo_key.c's own #error is the backstop.
OPENSSL_MIN := 3.2
ifneq ($(filter _build _san_test _san_bins,$(MAKECMDGOALS)),)
OPENSSL_PC_VERSION := $(shell pkg-config --modversion openssl 2>/dev/null)
ifneq ($(OPENSSL_PC_VERSION),)
ifneq ($(shell pkg-config --atleast-version=$(OPENSSL_MIN) openssl && echo ok),ok)
$(error OpenSSL $(OPENSSL_PC_VERSION) found; c-backup needs OpenSSL $(OPENSSL_MIN) or newer for Argon2id key derivation. See Manual.md section 2.1)
endif
endif
endif

# ---- Directory layout -------------------------------------------------
BUILD      := build
BINS       := $(BUILD)/bins
OBJ_DIR    := $(BUILD)/$(BUILD_TYPE)
SRC        := src
TESTS      := tests

# ---- Test scratch -----------------------------------------------------
# Every suite, the shell ones and the fuzzers included, writes its scratch
# under this one directory: compiled into the C suites (tests/test_tmp.h) and
# exported to the shell ones. On disk under the user's cache rather than in
# /tmp, which on a desktop is commonly a tmpfs: dense scratch there is RAM,
# and a suite that writes a few GiB stalls the machine. Per checkout, because
# two worktrees running `make test` at once otherwise share one set of fixed
# paths, and short, because two suites put unix sockets in it and sun_path
# holds 108 bytes. CI points it at the job directory.
XDG_CACHE_HOME    ?= $(HOME)/.cache
C_BACKUP_TEST_DIR ?= $(XDG_CACHE_HOME)/c-backup-test-$(shell printf '%s' '$(CURDIR)' | cksum | cut -d' ' -f1)
# The most scratch one suite may hold at once. tests/scratch-guard.sh runs
# each suite, polls the directory, and fails the suite that goes past this.
# Bytes, or with a K/M/G suffix.
C_BACKUP_SCRATCH_BUDGET ?= 10G
export C_BACKUP_TEST_DIR C_BACKUP_SCRATCH_BUDGET
SCRATCH_GUARD  := ./$(TESTS)/scratch-guard.sh
TEST_DIR_DEF   := -DC_BACKUP_TEST_DIR='"$(C_BACKUP_TEST_DIR)"'
# Rewritten only when the directory changes, so the suites relink then and
# not on every run. One per build type, so the asan and tsan suites can each
# run in a scratch directory of their own without relinking each other's.
TEST_DIR_STAMP := $(BUILD)/$(BUILD_TYPE)/test_dir.stamp

# ---- Flags ------------------------------------------------------------
WARN_FLAGS := -Wshadow -Wunused -Wunused-function -Wunused-variable \
              -Wunused-parameter -Wunused-result -Wdouble-promotion \
              -Wformat=2 -Wformat-truncation -Wmissing-prototypes \
              -Wstrict-prototypes -Wmissing-declarations -Wcast-align \
              -Wcast-qual -Wnull-dereference -Wconversion -Wsign-conversion
SRC_INCLUDES := -I src -I src/cli -I src/ops -I src/store -I src/api -I src/api/routes -I src/meta -I src/scheduler -I src/common \
                -I src/providers -I src/units -I src/helper -I src/virt \
                -I src/config -I src/daemon -I $(CUTILS_DIR)/include \
                -I $(CUTILS_DIR)/lib/cJSON
COMMON_CFLAGS := -std=c11 -pthread -Wall -Wextra -Wpedantic $(WARN_FLAGS) \
                 -fstack-protector-strong -fstack-clash-protection \
                 $(SRC_INCLUDES) $(VERSION_DEF) $(CUTILS_VERSION_DEF)

# The dev machine is Zen 3, so that is the default. CI and the container image
# override it, because a binary built for znver3 and run anywhere else dies on
# an illegal instruction rather than degrading. x86-64-v2 is the portable
# floor that still carries the SSE4.2 CRC32C intrinsics parity.c needs; the
# AVX2 paths there are behind __AVX2__ and simply do not compile in.
#
# MTUNE is separate because -mtune takes a CPU name, not an ISA level, so it
# cannot just follow an ISA-level MARCH.
MARCH ?= znver3
MTUNE ?= $(MARCH)
ARCH_CFLAGS := -march=$(MARCH) -mtune=$(MTUNE)

RELEASE_CFLAGS := $(COMMON_CFLAGS) -O3 -flto=auto $(ARCH_CFLAGS)
STATIC_CFLAGS  := $(COMMON_CFLAGS) -O3 -flto=auto $(ARCH_CFLAGS)
DEBUG_CFLAGS   := $(COMMON_CFLAGS) -Og -g $(ARCH_CFLAGS)
# The sanitizer builds. ASan and UBSan share one build, since they compose;
# TSan has its own, because it cannot be combined with ASan. UBSan does not
# recover: a report that lets the process carry on is a report a passing
# suite hides.
ASAN_SAN       := -fsanitize=address,undefined -fno-sanitize-recover=undefined
TSAN_SAN       := -fsanitize=thread
ASAN_CFLAGS    := $(COMMON_CFLAGS) -O1 -g $(ASAN_SAN) -fno-omit-frame-pointer $(ARCH_CFLAGS)
TSAN_CFLAGS    := $(COMMON_CFLAGS) -O1 -g $(TSAN_SAN) -fno-omit-frame-pointer $(ARCH_CFLAGS)

CUTILS_LIB     := -L$(CUTILS_DIR)/build -lc-utils
LDFLAGS        := $(CUTILS_LIB) -lmicrohttpd -llz4 -lssl -lcrypto -lacl -lsqlite3 -lcurl -pthread -lm

# A sanitizer runtime is linked statically where the toolchain ships the
# archive, so a sanitized daemon still runs on a host with no libasan.so
# (Unraid has none). Where it does not, the shared runtime: Arch's gcc ships
# only libasan.so, and -static-libasan there failed the link outright with
# "cannot find -lasan". gcc prints the bare name back when it has no such
# file, so an absolute path is the test.
static_rt = $(if $(filter /%,$(shell $(CC) -print-file-name=$(1))),$(2))

# Per-type CFLAGS/LDFLAGS selection
ifeq ($(BUILD_TYPE),release)
  TYPE_CFLAGS  := $(RELEASE_CFLAGS)
  TYPE_LDFLAGS := -flto=auto $(LDFLAGS)
  BIN_SUFFIX   :=
else ifeq ($(BUILD_TYPE),static)
  TYPE_CFLAGS  := $(STATIC_CFLAGS)
  TYPE_LDFLAGS := -flto=auto -Wl,-Bstatic $(LDFLAGS) -Wl,-Bdynamic
  BIN_SUFFIX   := -static
else ifeq ($(BUILD_TYPE),debug)
  TYPE_CFLAGS  := $(DEBUG_CFLAGS)
  TYPE_LDFLAGS := $(LDFLAGS)
  BIN_SUFFIX   := -debug
else ifeq ($(BUILD_TYPE),asan)
  TYPE_CFLAGS  := $(ASAN_CFLAGS)
  TYPE_LDFLAGS := $(call static_rt,libasan.a,-static-libasan) \
                  $(call static_rt,libubsan.a,-static-libubsan) $(LDFLAGS)
  SAN_CFLAGS   := $(ASAN_SAN)
  BIN_SUFFIX   := -asan
else ifeq ($(BUILD_TYPE),tsan)
  TYPE_CFLAGS  := $(TSAN_CFLAGS)
  TYPE_LDFLAGS := $(call static_rt,libtsan.a,-static-libtsan) $(LDFLAGS)
  SAN_CFLAGS   := $(TSAN_SAN)
  BIN_SUFFIX   := -tsan
endif

DAEMON_TARGET := $(BINS)/c-backupd$(BIN_SUFFIX)
CLI_TARGET    := $(BINS)/c-backup$(BIN_SUFFIX)
# The host helper (docs/source-providers.md 11): a separate privileged
# sidecar, built from the same objects with its own main.
HELPER_TARGET := $(BINS)/c-backup-helper$(BIN_SUFFIX)
TARGETS       := $(DAEMON_TARGET) $(CLI_TARGET) $(HELPER_TARGET)

# ---- Sources and objects ----------------------------------------------
# Sources live under src/{cli,ops,store,api,common}.  Object files are
# flattened into $(OBJ_DIR) by basename; VPATH lets the pattern rule find
# the .c under any subdirectory.
# Two entry points now: the daemon and the thin CLI client. Both are called
# main.c, and objects are flattened by basename, so they get explicit rules
# with distinct object names rather than going through the pattern rule.
DAEMON_MAIN := $(SRC)/daemon/main.c
CLI_MAIN    := $(SRC)/cli/main.c
HELPER_MAIN := $(SRC)/helper/helper_main.c
ALL_SRCS    := $(shell find $(SRC) -name '*.c')
# The libvirt provider lives in the helper alone (vm-backup.md 9, the
# approved dependency): src/virt is compiled with libvirt's and libxml2's
# flags and linked into c-backup-helper and its own tests, never into the
# daemon or the CLI. Without libvirt's development files it is left out and
# the helper offers no libvirt provider.
VIRT_SRCS   := $(wildcard $(SRC)/virt/*.c)
HAVE_LIBVIRT := $(shell pkg-config --exists libvirt libxml-2.0 2>/dev/null && echo 1)
ifeq ($(HAVE_LIBVIRT),1)
VIRT_CFLAGS := $(patsubst -I%,-isystem %,$(shell pkg-config --cflags libvirt libxml-2.0)) -DHAVE_LIBVIRT
VIRT_LIBS   := $(shell pkg-config --libs libvirt libxml-2.0)
else
VIRT_SRCS   :=
VIRT_CFLAGS :=
VIRT_LIBS   :=
endif
SRCS        := $(filter-out $(DAEMON_MAIN) $(CLI_MAIN) $(HELPER_MAIN) $(wildcard $(SRC)/virt/*.c), $(ALL_SRCS))
SRC_DIRS    := $(sort $(dir $(ALL_SRCS)))
VPATH       := $(SRC_DIRS)
# cJSON is not compiled here. c-utils carries it and libc-utils.a holds its
# object. A copy compiled here is linked ahead of the archive, so it shadows
# that member for the whole binary, c-utils' own json.o included: the daemon
# ran on whichever cJSON this repository happened to carry, not the one the
# pinned c-utils was tested with.
LIB_OBJS    := $(patsubst %.c, $(OBJ_DIR)/%.o, $(notdir $(SRCS))) \
               $(OBJ_DIR)/app_migrations.o
# Objects are flattened into one directory by basename, so two sources with
# the same filename in different subdirectories silently clobber each other
# and the link fails with an undefined reference to a function that is
# plainly right there. Caught twice; now it fails loudly at configure time.
DUP_BASENAMES := $(strip $(shell printf '%s\n' $(notdir $(SRCS)) | sort | uniq -d))
ifneq ($(DUP_BASENAMES),)
$(error duplicate source basenames would collide in $(OBJ_DIR): $(DUP_BASENAMES))
endif

DAEMON_OBJ  := $(OBJ_DIR)/daemon_main.o
CLI_OBJ     := $(OBJ_DIR)/cli_main.o
HELPER_OBJ  := $(OBJ_DIR)/helper_main.o
VIRT_OBJS   := $(patsubst %.c, $(OBJ_DIR)/%.o, $(notdir $(VIRT_SRCS)))
$(VIRT_OBJS) $(HELPER_OBJ): TYPE_CFLAGS += $(VIRT_CFLAGS)
OBJS        := $(LIB_OBJS) $(DAEMON_OBJ) $(CLI_OBJ) $(HELPER_OBJ)

# Generated code (the embedded migrations): match the optimisation level but
# drop the strict warnings, since nobody edits it by hand.
ifeq ($(BUILD_TYPE),debug)
  GEN_CFLAGS := -std=c11 -pthread -Og -g $(SRC_INCLUDES) $(VERSION_DEF)
else ifneq ($(SAN_CFLAGS),)
  GEN_CFLAGS := -std=c11 -pthread -O1 -g $(SAN_CFLAGS) $(SRC_INCLUDES) $(VERSION_DEF)
else
  GEN_CFLAGS := -std=c11 -pthread -O3 -flto=auto -march=$(MARCH) $(SRC_INCLUDES) $(VERSION_DEF)
endif

# ---- Compile-flag tracking --------------------------------------------
# -MMD records the headers an object read, not what it was compiled with, so
# on its own it keeps an object built under other flags. Most of all under a
# different CUTILS_DIR: 1.0.0 and v3 build against different c-utils trees,
# whose headers keep their old mtimes, so a switch between them rebuilt
# nothing and linked objects compiled against one c-utils with the other's
# archive. Each object tree keeps the flags it was built with in flags.stamp,
# rewritten only when they change, and every object and binary in the tree
# depends on it. The version defines are left out: they carry the build
# minute and would rebuild everything every minute.
FLAG_SIG_STRIP := $(VERSION_DEF) $(CUTILS_VERSION_DEF)
flag_sig = $(strip $(CC) $(shell $(CC) -dumpfullversion) $(filter-out $(FLAG_SIG_STRIP),$(1)))

# $(1) the object directory, $(2) the name of the variable holding its
# signature (by name, so a comma in the flags cannot split the ifneq).
define track_flags
ifeq ($$(filter clean,$$(MAKECMDGOALS)),)
ifneq ($$(file < $(1)/flags.stamp),$$($(2)))
$$(shell mkdir -p $(1))
$$(file > $(1)/flags.stamp,$$($(2)))
endif
endif
endef

# A tree whose stamp was not written at parse time (its goal was reached
# some other way) gets one written when something first needs it.
define stamp_rule
$(1)/flags.stamp:
	@mkdir -p $$(@D)
	$$(file > $$@,$$($(2)))
endef

FLAGS_STAMP := $(OBJ_DIR)/flags.stamp
FLAGS_SIG   := $(call flag_sig,$(TYPE_CFLAGS) $(TYPE_LDFLAGS) $(GEN_CFLAGS))
$(eval $(call track_flags,$(OBJ_DIR),FLAGS_SIG))
$(eval $(call stamp_rule,$(OBJ_DIR),FLAGS_SIG))

# The headers only the suites include. A suite is compiled in the same
# command that links it, so these are its whole header dependency on the
# test side; the library side comes through the objects it links.
TEST_HDRS := $(wildcard $(TESTS)/*.h)

# ---- Phony targets ----------------------------------------------------
.PHONY: all static asan tsan debug clean test bench bench-micro bench-phases bench-browse bench-nbd analyze lint check-conflict-markers

# In the top-level make, which is the release build unless told otherwise,
# `all` is built here rather than by a sub-make (BUILD-1). The test binaries
# link build/release objects through this make's own pattern rule, so a
# recursive `all` gave two make processes a rule for the same objects:
# `make -j test` on a dirty tree compiled one twice at once, and an LTO link
# reading it while it was rewritten failed with "lto1: internal compiler
# error: resolution sub id ... not in object file". One make, one rule per
# object, and the jobserver orders the rest.
ifeq ($(BUILD_TYPE),release)
all: _build
else
all:
	$(MAKE) BUILD_TYPE=release _build
endif

static:
	$(MAKE) BUILD_TYPE=static _build

debug:
	$(MAKE) BUILD_TYPE=debug _build

asan:
	$(MAKE) BUILD_TYPE=asan _build

tsan:
	$(MAKE) BUILD_TYPE=tsan _build

# ---- Internal build target (called via recursive make) ----------------
.PHONY: _build
_build: $(MIGRATE_SRC) $(TARGETS)

$(BUILD):
	mkdir -p $(BUILD)

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

$(BINS):
	mkdir -p $(BINS)

$(OBJ_DIR)/%.o: %.c $(FLAGS_STAMP) | $(OBJ_DIR)
	$(CC) $(TYPE_CFLAGS) -MMD -MP -c $< -o $@

# ---- Embedded SQL migrations ------------------------------------------
# Compiled into the binary so a deployed daemon carries its own schema and
# cannot half-migrate against a stale migrations/ directory on disk.
MIGRATE_SRC := $(BUILD)/app_migrations.c

#
# The directory is a prerequisite as well as its files. A migration removed
# by switching branches changes no remaining file, only the directory, and
# without it a stale embed carried migrations the checkout no longer has.
$(MIGRATE_SRC): migrations $(wildcard migrations/*.sql) | $(BUILD)
	$(CUTILS_DIR)/tools/embed_sql.sh migrations/ app_migrations > $@

$(OBJ_DIR)/app_migrations.o: $(MIGRATE_SRC) $(FLAGS_STAMP) | $(OBJ_DIR)
	$(CC) $(GEN_CFLAGS) -MMD -MP -c $< -o $@

.PHONY: embed-migrations
embed-migrations: $(MIGRATE_SRC)

$(DAEMON_OBJ): $(DAEMON_MAIN) $(FLAGS_STAMP) | $(OBJ_DIR)
	$(CC) $(TYPE_CFLAGS) -MMD -MP -c $< -o $@
$(CLI_OBJ): $(CLI_MAIN) $(FLAGS_STAMP) | $(OBJ_DIR)
	$(CC) $(TYPE_CFLAGS) -MMD -MP -c $< -o $@

-include $(wildcard $(OBJ_DIR)/*.d)

# The c-utils archive is a prerequisite so a rebuilt c-utils relinks the
# binaries; without it they kept the old library and a c-utils fix looked
# like it had not worked. Wildcard, so a missing archive still reaches the
# linker's own error.
CUTILS_ARCHIVE := $(wildcard $(CUTILS_DIR)/build/libc-utils.a)

$(DAEMON_TARGET): $(LIB_OBJS) $(DAEMON_OBJ) $(CUTILS_ARCHIVE) $(FLAGS_STAMP) | $(BINS)
	$(CC) $(TYPE_CFLAGS) $(filter %.o %.a,$^) -o $@ $(TYPE_LDFLAGS)

$(CLI_TARGET): $(LIB_OBJS) $(CLI_OBJ) $(CUTILS_ARCHIVE) $(FLAGS_STAMP) | $(BINS)
	$(CC) $(TYPE_CFLAGS) $(filter %.o %.a,$^) -o $@ $(TYPE_LDFLAGS)

$(HELPER_TARGET): $(LIB_OBJS) $(VIRT_OBJS) $(HELPER_OBJ) $(CUTILS_ARCHIVE) $(FLAGS_STAMP) | $(BINS)
	$(CC) $(TYPE_CFLAGS) $(filter %.o %.a,$^) -o $@ $(TYPE_LDFLAGS) $(VIRT_LIBS)

# ---- Tests (link against release lib objects) -------------------------
REL_OBJ_DIR := $(BUILD)/release
REL_LIB_OBJS = $(patsubst %.c, $(REL_OBJ_DIR)/%.o, $(notdir $(SRCS))) \
               $(REL_OBJ_DIR)/app_migrations.o

TEST_CFLAGS  := -std=c11 -pthread -Wall -Wextra -O2 -march=$(MARCH) -Wno-unused-result $(SRC_INCLUDES)
TEST_LDFLAGS := -flto=auto $(LDFLAGS) -lcmocka

# Every suite in tests/ is live. Helpers are linked into suites rather than
# built as suites of their own; a test_*_fault.c suite additionally links the
# fault injector and its --wrap flags.
TEST_HELPERS := $(TESTS)/fault_inject.c $(TESTS)/fake_index.c
FAULT_SRCS  := $(wildcard $(TESTS)/test_*_fault.c)
TEST_SRCS   := $(filter-out $(FAULT_SRCS) $(TEST_HELPERS), $(wildcard $(TESTS)/*.c))
TEST_BINS   := $(patsubst $(TESTS)/%.c, $(BINS)/%, $(TEST_SRCS))
FAULT_BINS  := $(patsubst $(TESTS)/%.c, $(BINS)/%, $(FAULT_SRCS))

FAULT_WRAP := -Wl,--wrap=malloc,--wrap=calloc,--wrap=realloc \
              -Wl,--wrap=fread,--wrap=fwrite,--wrap=fflush,--wrap=fseeko,--wrap=fsync,--wrap=fdatasync \
              -Wl,--wrap=sync_file_range,--wrap=mkstemp,--wrap=rename,--wrap=read,--wrap=pread \
              -Wl,--wrap=pwrite,--wrap=unlink,--wrap=syscall,--wrap=write,--wrap=mkdir \
              -Wl,--wrap=fclose,--wrap=close,--wrap=renameat,--wrap=ftruncate,--wrap=syncfs

# Ensure release objects exist before linking tests
$(TEST_DIR_STAMP): FORCE
	@mkdir -p $(dir $@)
	@[ "$$(cat $@ 2>/dev/null)" = '$(C_BACKUP_TEST_DIR)' ] || echo '$(C_BACKUP_TEST_DIR)' > $@

.PHONY: FORCE
FORCE:

$(BINS)/%: $(TESTS)/%.c $(TESTS)/fake_index.c $(REL_LIB_OBJS) $(TEST_HDRS) $(TEST_DIR_STAMP) | $(BINS)
	$(CC) $(TEST_CFLAGS) $(TEST_DIR_DEF) $(filter %.c %.o,$^) -o $@ $(TEST_LDFLAGS)

# Suites of the libvirt provider link it and libvirt; they exist only where
# libvirt does.
REL_VIRT_OBJS = $(patsubst %.c, $(REL_OBJ_DIR)/%.o, $(notdir $(VIRT_SRCS)))
$(REL_VIRT_OBJS): TYPE_CFLAGS += $(VIRT_CFLAGS)
ifneq ($(HAVE_LIBVIRT),1)
TEST_SRCS := $(filter-out $(wildcard $(TESTS)/test_virt*.c), $(TEST_SRCS))
TEST_BINS := $(patsubst $(TESTS)/%.c, $(BINS)/%, $(TEST_SRCS))
endif
# A static pattern rule, not a pattern one (BUILD-2). As `$(BINS)/test_virt%`
# it competed with the generic `$(BINS)/%` above: make takes a pattern rule
# only if it can settle every prerequisite during its rule search, and when
# it could not settle the libvirt objects it fell back to the generic rule,
# which links neither them nor libvirt, so a clean parallel build failed to
# link test_virt_offline and test_virt_incremental. A static pattern rule
# names its targets, so nothing else can build them.
VIRT_TEST_BINS := $(patsubst $(TESTS)/%.c, $(BINS)/%, $(wildcard $(TESTS)/test_virt*.c))
ifeq ($(HAVE_LIBVIRT),1)
$(VIRT_TEST_BINS): $(BINS)/%: $(TESTS)/%.c $(TESTS)/fake_index.c $(REL_LIB_OBJS) $(REL_VIRT_OBJS) $(TEST_HDRS) $(TEST_DIR_STAMP) | $(BINS)
	$(CC) $(TEST_CFLAGS) $(VIRT_CFLAGS) $(TEST_DIR_DEF) $(filter %.c %.o,$^) -o $@ $(TEST_LDFLAGS) $(VIRT_LIBS)
else
$(VIRT_TEST_BINS):
	@echo "$@ needs libvirt's and libxml2's development files (pkg-config libvirt libxml-2.0)" >&2; exit 1
endif

$(BINS)/test_%_fault: $(TESTS)/test_%_fault.c $(TESTS)/fault_inject.c $(TESTS)/fake_index.c $(REL_LIB_OBJS) $(TEST_HDRS) $(TEST_DIR_STAMP) | $(BINS)
	$(CC) $(TEST_CFLAGS) $(TEST_DIR_DEF) $(FAULT_WRAP) $(filter %.c %.o,$^) -o $@ $(TEST_LDFLAGS)

# The shell suite drives a real daemon over HTTP. The unit tests cover the
# code behind the routes; nothing else covers the routes themselves, the auth
# middleware, or the job manager running work on a worker thread, because
# those only exist once a process is listening.
#
# The shell suites take their TCP ports from free-port, which asks the
# kernel for ones nothing holds (FLAKE-13), so concurrent worktrees and CI
# jobs on one host never meet on a port. Not part of the product, so built
# here rather than under src/, with the same strict flags.
FREE_PORT := $(BINS)/free-port
export C_BACKUP_FREE_PORT := $(abspath $(FREE_PORT))
$(FREE_PORT): $(TESTS)/tools/free_port.c | $(BINS)
	$(CC) $(COMMON_CFLAGS) -O2 $< -o $@

.PHONY: test-api
test-api: all $(FREE_PORT)
	@$(SCRATCH_GUARD) test_api_e2e.sh ./$(TESTS)/test_api_e2e.sh $(BINS)/c-backupd

# The CLI holds no repository logic, so what is worth testing is that each
# command reaches the right endpoint and that a missing daemon produces a
# clear message rather than a crash or a hang.
# B134 at UAT's sizes: one 300 MB, 600 MB and 2 GiB file each into a fresh
# repository at the production pack seal. About 2.5 GiB of scratch, so it is
# a nightly target, not part of make test (which runs the same rollover
# logic at an 8 MiB seal in test_pack_rollover).
.PHONY: test-rollover-full
test-rollover-full: $(BINS)/test_pack_rollover
	@ROLLOVER_FULL=1 $(SCRATCH_GUARD) test_pack_rollover_full $(BINS)/test_pack_rollover

.PHONY: test-cli
test-cli: all $(FREE_PORT)
	@$(SCRATCH_GUARD) test_cli_e2e.sh ./$(TESTS)/test_cli_e2e.sh $(BINS)/c-backupd $(BINS)/c-backup

# Units through the API and the CLI, with the fake provider enabled by its
# test-only config key: routes, the job, units.allow, restore, schedules.
.PHONY: test-units
test-units: all
	@$(SCRATCH_GUARD) test_units_e2e.sh ./$(TESTS)/test_units_e2e.sh $(BINS)/c-backupd $(BINS)/c-backup
	@$(SCRATCH_GUARD) test_helper_e2e.sh ./$(TESTS)/test_helper_e2e.sh $(BINS)/c-backupd $(BINS)/c-backup $(BINS)/c-backup-helper

# The shell suites each start a daemon and poll jobs on a wall-clock budget,
# so anything running beside them under -j competes for CPU and disk and makes
# them fail on timing rather than on behaviour. analyze is the worst offender
# because cppcheck and -fanalyzer saturate every core. Chain all three.
# The ui contract joins the chain too: every suite shares one scratch
# directory, and the guard can only attribute its growth, and tell a
# suite's leftovers from another's live files, when one runs at a time.
test-api: analyze
test-cli: test-api
test-units: test-cli
test-ui: test-units

# || exit 1, because without it a failing unit suite is invisible: cmocka
# exits non-zero, the loop swallows it, make reports success, and every gate
# check downstream believes the tree is green. The shell suites and the
# analyzers always failed the target properly; the C ones never did.
# The desktop app's API contract.
#
# It is the second client of the same daemon and until now was held to
# nothing, which is how it spent four months posting to a route that has
# never existed. This asks a running daemon whether every request the app can
# build names something real, so a path that stops existing fails here rather
# than in somebody's backup.
#
# Skipped rather than failed when node is absent: a C build on a machine
# without a JS toolchain is a reasonable thing to do, and pretending
# otherwise would make the C suite depend on it.
.PHONY: test-ui
test-ui: all
	@if command -v node >/dev/null 2>&1 && [ -d ui/tests ]; then \
	    $(SCRATCH_GUARD) "ui contract" \
	        sh -c 'cd ui && TMPDIR="$$C_BACKUP_TEST_DIR" npm test' || exit 1; \
	else \
	    echo "ui contract: skipped (no node)"; \
	fi

test: all $(TEST_BINS) $(FAULT_BINS) analyze test-api test-cli test-units test-ui
	@for t in $(TEST_BINS) $(FAULT_BINS); do $(SCRATCH_GUARD) $$(basename $$t) $$t || exit 1; done

# ---- Sanitizer tests --------------------------------------------------
#
# make test-asan (ASan + UBSan) and make test-tsan: every unit and fault
# suite, then the API, CLI, units and helper end-to-end suites against a
# daemon, CLI and helper built with the same sanitizer.
#
# The suites are built by the sanitizer's own sub-make, the one with rules
# and .d files for its objects (BUILD-3). They used to be linked by this
# make, which has no rule for anything under build/asan: it never rebuilt an
# object there, so `make asan-test-bins` after a branch switch compiled each
# suite against today's headers and linked it to objects compiled against
# last week's. A struct that had grown in between read as a
# stack-buffer-overflow that was not there.
SAN_TYPES := asan tsan

# One suite, or a few, by name: make test-tsan SAN_ONLY='test_db_txn test_api_e2e.sh'
SAN_ONLY ?=

ifneq ($(filter $(BUILD_TYPE),$(SAN_TYPES)),)
SAN_TEST_CFLAGS := -std=c11 -pthread -Wall -Wextra -O1 -g -fno-omit-frame-pointer $(SAN_CFLAGS) \
                   -Wno-unused-result $(SRC_INCLUDES) $(CUTILS_VERSION_DEF)
SAN_VIRT_SRCS   := $(filter $(TESTS)/test_virt%,$(TEST_SRCS))
SAN_TEST_BINS   := $(patsubst $(TESTS)/%.c, $(BINS)/%$(BIN_SUFFIX), $(filter-out $(SAN_VIRT_SRCS),$(TEST_SRCS)))
SAN_VIRT_BINS   := $(patsubst $(TESTS)/%.c, $(BINS)/%$(BIN_SUFFIX), $(SAN_VIRT_SRCS))
SAN_FAULT_BINS  := $(patsubst $(TESTS)/%.c, $(BINS)/%$(BIN_SUFFIX), $(FAULT_SRCS))

# Static pattern rules, so nothing else in this file can claim the targets.
$(SAN_TEST_BINS): $(BINS)/%$(BIN_SUFFIX): $(TESTS)/%.c $(TESTS)/fake_index.c $(LIB_OBJS) $(CUTILS_ARCHIVE) \
                  $(TEST_HDRS) $(FLAGS_STAMP) $(TEST_DIR_STAMP) | $(BINS)
	$(CC) $(SAN_TEST_CFLAGS) $(TEST_DIR_DEF) $(filter %.c %.o %.a,$^) -o $@ $(TYPE_LDFLAGS) -lcmocka

# The libvirt suites, like their release rule, link the provider and libvirt.
$(SAN_VIRT_BINS): $(BINS)/%$(BIN_SUFFIX): $(TESTS)/%.c $(TESTS)/fake_index.c $(LIB_OBJS) $(VIRT_OBJS) $(CUTILS_ARCHIVE) \
                  $(TEST_HDRS) $(FLAGS_STAMP) $(TEST_DIR_STAMP) | $(BINS)
	$(CC) $(SAN_TEST_CFLAGS) $(VIRT_CFLAGS) $(TEST_DIR_DEF) $(filter %.c %.o %.a,$^) -o $@ $(TYPE_LDFLAGS) $(VIRT_LIBS) -lcmocka

$(SAN_FAULT_BINS): $(BINS)/%$(BIN_SUFFIX): $(TESTS)/%.c $(TESTS)/fault_inject.c $(TESTS)/fake_index.c $(LIB_OBJS) \
                   $(CUTILS_ARCHIVE) $(TEST_HDRS) $(FLAGS_STAMP) $(TEST_DIR_STAMP) | $(BINS)
	$(CC) $(SAN_TEST_CFLAGS) $(TEST_DIR_DEF) $(FAULT_WRAP) $(filter %.c %.o %.a,$^) -o $@ $(TYPE_LDFLAGS) -lcmocka

# Reports go to files, one directory per suite, as well as failing the
# suite. The end-to-end suites write a daemon's output into a scratch
# directory they delete, and compare what a CLI printed against what they
# expect, so a report on stderr was either lost or read as a wrong answer.
# A directory left non-empty fails the suite even when the suite passed: a
# TSan race or a leak at a daemon's exit does not change its exit status.
#
# UBSan is the exception: gcc links it as a second runtime beside libasan,
# with its own copy of the report plumbing that never reads log_path, so its
# reports go to stderr whatever the options say. It does not recover, so the
# process it fires in exits non-zero and the suite fails; in an end-to-end
# suite that is a daemon dying mid-run, and its report is in the daemon log.
SAN_REPORTS := $(abspath $(OBJ_DIR))/reports
SAN_ENV_asan := ASAN_OPTIONS=detect_leaks=1:detect_stack_use_after_return=1:check_initialization_order=1:strict_init_order=1:log_path=$$d/asan \
                UBSAN_OPTIONS=print_stacktrace=1:log_path=$$d/ubsan \
                LSAN_OPTIONS=suppressions=$(abspath $(TESTS)/lsan.supp):print_suppressions=0
SAN_ENV_tsan := TSAN_OPTIONS=second_deadlock_stack=1:history_size=4:log_path=$$d/tsan
SAN_RUN_UNIT := $(if $(SAN_ONLY),$(filter $(addprefix $(BINS)/,$(addsuffix $(BIN_SUFFIX),$(SAN_ONLY))),$(SAN_TEST_BINS) $(SAN_VIRT_BINS) $(SAN_FAULT_BINS)),$(SAN_TEST_BINS) $(SAN_VIRT_BINS) $(SAN_FAULT_BINS))
san_e2e = $(if $(SAN_ONLY),$(filter $(1),$(SAN_ONLY)),$(1))

.PHONY: _san_bins _san_test
_san_bins: $(SAN_TEST_BINS) $(SAN_VIRT_BINS) $(SAN_FAULT_BINS)

# Every suite runs, and the target fails at the end naming each one that
# failed: a sweep that stops at its first finding hides the rest.
_san_test: _build $(SAN_RUN_UNIT) $(FREE_PORT)
	@rm -rf $(SAN_REPORTS); fail=""; \
	run() { n=$$1; shift; d=$(SAN_REPORTS)/$$n; mkdir -p "$$d"; rc=0; \
	    env $(SAN_ENV_$(BUILD_TYPE)) $(SCRATCH_GUARD) "$$n" "$$@" || rc=1; \
	    if [ -n "$$(ls -A "$$d")" ]; then echo "$(BUILD_TYPE): $$n left reports in $$d"; rc=1; fi; \
	    [ $$rc -eq 0 ] || fail="$$fail $$n"; }; \
	for t in $(SAN_RUN_UNIT); do run $$(basename $$t $(BIN_SUFFIX)) $$t; done; \
	for s in $(call san_e2e,test_api_e2e.sh); do run $$s ./$(TESTS)/$$s $(DAEMON_TARGET); done; \
	for s in $(call san_e2e,test_cli_e2e.sh test_units_e2e.sh); do run $$s ./$(TESTS)/$$s $(DAEMON_TARGET) $(CLI_TARGET); done; \
	for s in $(call san_e2e,test_helper_e2e.sh); do run $$s ./$(TESTS)/$$s $(DAEMON_TARGET) $(CLI_TARGET) $(HELPER_TARGET); done; \
	if [ -n "$$fail" ]; then echo "$(BUILD_TYPE) failed:$$fail"; exit 1; fi; \
	echo "$(BUILD_TYPE): every suite clean"
endif

test-asan:
	$(MAKE) BUILD_TYPE=asan _san_test

test-tsan:
	$(MAKE) BUILD_TYPE=tsan _san_test

asan-test-bins:
	$(MAKE) BUILD_TYPE=asan _san_bins

tsan-test-bins:
	$(MAKE) BUILD_TYPE=tsan _san_bins

.PHONY: test-asan test-tsan asan-test-bins tsan-test-bins

# ---- Fuzzing ----------------------------------------------------------
#
# libFuzzer under clang, with ASan and UBSan, in its own build/fuzz tree.
# Clang rather than the gcc the rest of the build uses, because libFuzzer is
# an LLVM component; the strict-flag guarantees still come from the ordinary
# build, this one exists to run the parsers against bytes nobody intended.
#
# The targets are the places where data that came off a disk becomes
# structure, before anything has checked a hash. That is the whole surface
# worth fuzzing in a program whose job is reading back arbitrary bytes it
# wrote months ago: a corrupt length field is not an attacker, it is a
# Tuesday.
FUZZ_CC       ?= clang
FUZZ_OBJ_DIR  := $(BUILD)/fuzz
FUZZ_BIN_DIR  := $(BUILD)/fuzz/bin
FUZZ_SRCS     := $(wildcard $(TESTS)/fuzz/fuzz_*.c)
FUZZ_BINS     := $(patsubst $(TESTS)/fuzz/%.c, $(FUZZ_BIN_DIR)/%, $(FUZZ_SRCS))
# fake_index stands in for the daemon's pack bookkeeping. A harness that
# packs anything needs it: since the .idx sidecar went away the engine has
# no index of its own and asks a callback where a hash lives, so without one
# a packed object is simply not found.
#
# cJSON is the one c-utils source compiled here, from c-utils' own tree, so
# the sanitizers and the fuzzer's coverage reach it. Defined in an object, it
# wins over the archive's member, which then never gets pulled in.
CUTILS_CJSON_SRC := $(CUTILS_DIR)/lib/cJSON/cJSON.c
FUZZ_LIB_OBJS := $(patsubst %.c, $(FUZZ_OBJ_DIR)/%.o, $(notdir $(SRCS))) \
                 $(FUZZ_OBJ_DIR)/cJSON.o $(FUZZ_OBJ_DIR)/app_migrations.o \
                 $(FUZZ_OBJ_DIR)/fake_index.o

FUZZ_SAN    := -fsanitize=fuzzer-no-link,address,undefined -fno-omit-frame-pointer -g
FUZZ_CFLAGS := -std=c11 -pthread -Wall -Wextra -O1 -Wno-unused-result $(FUZZ_SAN) \
               $(SRC_INCLUDES) -I $(TESTS) $(CUTILS_VERSION_DEF)
FUZZ_LDFLAGS := -fsanitize=fuzzer,address,undefined \
                $(CUTILS_LIB) -lmicrohttpd -llz4 -lssl -lcrypto -lacl -lsqlite3 -lcurl -pthread -lm

$(FUZZ_OBJ_DIR) $(FUZZ_BIN_DIR):
	mkdir -p $@

FUZZ_STAMP := $(FUZZ_OBJ_DIR)/flags.stamp
FUZZ_SIG   := $(call flag_sig,$(FUZZ_CC) $(FUZZ_CFLAGS) $(FUZZ_LDFLAGS))
ifneq ($(filter fuzz%,$(MAKECMDGOALS)),)
$(eval $(call track_flags,$(FUZZ_OBJ_DIR),FUZZ_SIG))
endif
$(eval $(call stamp_rule,$(FUZZ_OBJ_DIR),FUZZ_SIG))

define FUZZ_OBJ_RULE
$(FUZZ_OBJ_DIR)/$(notdir $(basename $(1))).o: $(1) $(FUZZ_STAMP) | $(FUZZ_OBJ_DIR)
	$$(FUZZ_CC) $$(FUZZ_CFLAGS) -MMD -MP -c $$< -o $$@
endef
$(foreach s,$(SRCS),$(eval $(call FUZZ_OBJ_RULE,$(s))))

$(FUZZ_OBJ_DIR)/cJSON.o: $(CUTILS_CJSON_SRC) $(FUZZ_STAMP) | $(FUZZ_OBJ_DIR)
	$(FUZZ_CC) $(FUZZ_CFLAGS) -DCUTILS_CJSON_ALLOW -Wno-everything -MMD -MP -c $< -o $@

$(FUZZ_OBJ_DIR)/app_migrations.o: $(MIGRATE_SRC) $(FUZZ_STAMP) | $(FUZZ_OBJ_DIR)
	$(FUZZ_CC) $(FUZZ_CFLAGS) -MMD -MP -c $< -o $@

$(FUZZ_OBJ_DIR)/fake_index.o: $(TESTS)/fake_index.c $(FUZZ_STAMP) | $(FUZZ_OBJ_DIR)
	$(FUZZ_CC) $(FUZZ_CFLAGS) -I $(TESTS) -MMD -MP -c $< -o $@

-include $(wildcard $(FUZZ_OBJ_DIR)/*.d)

$(FUZZ_BIN_DIR)/%: $(TESTS)/fuzz/%.c $(FUZZ_LIB_OBJS) $(TEST_HDRS) $(FUZZ_STAMP) $(TEST_DIR_STAMP) | $(FUZZ_BIN_DIR)
	$(FUZZ_CC) $(FUZZ_CFLAGS) $(TEST_DIR_DEF) $(filter %.c %.o,$^) -o $@ $(FUZZ_LDFLAGS)

.PHONY: fuzz
fuzz: $(MIGRATE_SRC) $(FUZZ_BINS)
	@echo "fuzz targets: $(notdir $(FUZZ_BINS))"

# Seeds come from a real repository rather than from imagination: a fuzzer
# handed valid structures finds the interesting mutations, one starting from
# nothing spends its budget rediscovering magic numbers.
FUZZ_CORPUS := $(TESTS)/fuzz/corpus
FUZZ_DICT   := $(TESTS)/fuzz/dict

# CI budget per target. A soak is the same command with a bigger number, run
# by hand: make fuzz-run FUZZ_TIME=3600
FUZZ_TIME ?= 60
FUZZ_JOBS ?= 1

.PHONY: fuzz-run
fuzz-run: fuzz
	@rc=0; \
	for t in $(FUZZ_BINS); do \
	    n=$$(basename $$t); \
	    mkdir -p $(FUZZ_CORPUS)/$$n; \
	    d=""; [ -f $(FUZZ_DICT)/$$n.dict ] && d="-dict=$(FUZZ_DICT)/$$n.dict"; \
	    C_BACKUP_SCRATCH_LEFTOVERS=remove $(SCRATCH_GUARD) "$$n ($(FUZZ_TIME)s)" \
	    $$t $(FUZZ_CORPUS)/$$n $$d -max_total_time=$(FUZZ_TIME) \
	        -jobs=$(FUZZ_JOBS) -workers=$(FUZZ_JOBS) -timeout=25 \
	        -rss_limit_mb=2048 -print_final_stats=1 -error_exitcode=77 || rc=1; \
	done; \
	exit $$rc

# Shrink the corpus to the smallest set that still reaches everything the
# big one did. A fuzzing run leaves hundreds of inputs that are variations on
# each other; committing those costs repository size forever and buys nothing,
# because the coverage is what matters and it is already covered.
.PHONY: fuzz-min
fuzz-min: fuzz
	@for t in $(FUZZ_BINS); do \
	    n=$$(basename $$t); \
	    [ -d $(FUZZ_CORPUS)/$$n ] || continue; \
	    tmp=$$(mktemp -d); \
	    before=$$(find $(FUZZ_CORPUS)/$$n -type f | wc -l); \
	    $$t -merge=1 -timeout=25 $$tmp $(FUZZ_CORPUS)/$$n >/dev/null 2>&1 || true; \
	    rm -rf $(FUZZ_CORPUS)/$$n; mv $$tmp $(FUZZ_CORPUS)/$$n; \
	    after=$$(find $(FUZZ_CORPUS)/$$n -type f | wc -l); \
	    echo "$$n: $$before -> $$after"; \
	done

# What the corpus actually reaches.
#
# A separate clang build with source-based coverage instead of libFuzzer's
# own counters, because the question is which lines of the parsers the corpus
# exercises, not how the fuzzer is doing. Replays every committed input and
# reports per-function.
#
# Read it per function, never as a total: these binaries link the whole
# daemon, so a whole-binary figure is dominated by the routes, the scheduler
# and the job manager, which no fuzz target has any business reaching.
#
# This number stays out of `make coverage` and out of the gate it enforces,
# deliberately. Merging them would raise the figure without raising
# confidence, because the two measure different things: a unit test asserts
# what a line does, a fuzz harness mostly asserts that a round trip survived
# and that nothing faulted. Lines the corpus merely executes would count the
# same as lines something checked.
#
# The worse effect is on the ratchet. A gate exists to notice when cover is
# lost - and if the corpus also lights up a line, deleting the test that
# actually asserted its behaviour leaves the number unchanged. The gate
# would go quiet exactly when it should complain.
#
# So: two numbers, two purposes. `make coverage` gates, this reports.
FUZZCOV_DIR  := $(BUILD)/fuzzcov
FUZZCOV_OBJ  := $(FUZZCOV_DIR)/obj
FUZZCOV_BIN  := $(FUZZCOV_DIR)/bin
FUZZCOV_CF   := -std=c11 -pthread -O1 -g -fsanitize=fuzzer-no-link \
                -fprofile-instr-generate -fcoverage-mapping -Wno-unused-result \
                $(SRC_INCLUDES) -I $(TESTS) $(CUTILS_VERSION_DEF)
LLVM_PROFDATA ?= llvm-profdata
LLVM_COV      ?= llvm-cov

.PHONY: fuzz-cov
fuzz-cov: $(MIGRATE_SRC)
	@mkdir -p $(FUZZCOV_OBJ) $(FUZZCOV_BIN)
	@for f in $(SRCS); do \
	    $(FUZZ_CC) $(FUZZCOV_CF) -c $$f -o $(FUZZCOV_OBJ)/$$(basename $${f%.c}).o || exit 1; \
	done
	@$(FUZZ_CC) $(FUZZCOV_CF) -DCUTILS_CJSON_ALLOW -Wno-everything -c $(CUTILS_CJSON_SRC) -o $(FUZZCOV_OBJ)/cJSON.o
	@$(FUZZ_CC) $(FUZZCOV_CF) -c $(MIGRATE_SRC) -o $(FUZZCOV_OBJ)/app_migrations.o
	@$(FUZZ_CC) $(FUZZCOV_CF) -c $(TESTS)/fake_index.c -o $(FUZZCOV_OBJ)/fake_index.o
	@for t in $(FUZZ_SRCS); do \
	    n=$$(basename $${t%.c}); \
	    $(FUZZ_CC) $(FUZZCOV_CF) $(TEST_DIR_DEF) $$t $(FUZZCOV_OBJ)/*.o -o $(FUZZCOV_BIN)/$$n \
	        -fsanitize=fuzzer -fprofile-instr-generate -fcoverage-mapping \
	        $(CUTILS_LIB) -lmicrohttpd -llz4 -lssl -lcrypto -lacl -lsqlite3 -lcurl -pthread -lm || exit 1; \
	done
	@rm -rf $(FUZZCOV_DIR)/prof && mkdir -p $(FUZZCOV_DIR)/prof
	@for t in $(FUZZCOV_BIN)/*; do \
	    n=$$(basename $$t); \
	    [ -d $(FUZZ_CORPUS)/$$n ] || continue; \
	    LLVM_PROFILE_FILE=$(FUZZCOV_DIR)/prof/$$n.profraw \
	        $$t -runs=0 $(FUZZ_CORPUS)/$$n >/dev/null 2>&1 || true; \
	done
	@$(LLVM_PROFDATA) merge -sparse $(FUZZCOV_DIR)/prof/*.profraw \
	    -o $(FUZZCOV_DIR)/fuzz.profdata
	@first=$$(ls $(FUZZCOV_BIN)/* | head -1); \
	 rest=""; for t in $(FUZZCOV_BIN)/*; do [ "$$t" = "$$first" ] || rest="$$rest -object $$t"; done; \
	 for f in $(FUZZ_COV_FILES); do \
	    echo "=== $$f ==="; \
	    $(LLVM_COV) report $$first -instr-profile=$(FUZZCOV_DIR)/fuzz.profdata $$rest \
	        -show-functions $$f 2>/dev/null \
	      | awk 'NR>2 && $$1!~/^-/ && $$1!="TOTAL" && $$1!~/^fuzz_/ {printf "  %-44s %8s\n", $$1, $$(NF-3)}'; \
	 done

# The files a fuzz target is actually aimed at. Listed rather than globbed:
# the point is to see whether the parsers are covered, and a list that has to
# be edited when a target is added is a list somebody has looked at.
FUZZ_COV_FILES := src/common/metablob.c src/store/manifest.c \
                  src/ops/sparse_restore.c src/store/parity.c \
                  src/store/parity_stream.c src/store/object.c \
                  src/store/object_crypt.c src/store/pack.c \
                  src/ops/replicate.c src/ops/replica_local.c

# Replay the committed corpus and stop. This is the regression check: it
# runs every input a fuzzer ever found interesting, in seconds, without
# searching for new ones. Suitable for a pipeline that must not take
# minutes.
.PHONY: fuzz-regress
fuzz-regress: fuzz
	@rc=0; \
	for t in $(FUZZ_BINS); do \
	    n=$$(basename $$t); \
	    if [ -d $(FUZZ_CORPUS)/$$n ]; then \
	        C_BACKUP_SCRATCH_LEFTOVERS=remove $(SCRATCH_GUARD) "$$n (replay)" \
	            $$t -runs=0 -error_exitcode=77 $(FUZZ_CORPUS)/$$n || rc=1; \
	    fi; \
	done; \
	exit $$rc

# ---- Static analysis --------------------------------------------------
ANALYZE_CFLAGS := -std=c11 -pthread -Wall -Wextra -O2 -march=$(MARCH) $(SRC_INCLUDES) $(CUTILS_VERSION_DEF)
STACK_LIMIT    := 65536

# A merge that commits both sides of a conflict builds and passes tests when
# the markers land in prose (docs/repo-format.md did, in the OWN-46 merge), so
# look for them in every tracked file. Only the seven-character opening and
# closing markers count: a line of '=' is a legitimate Markdown heading rule.
check-conflict-markers:
	@echo "=== conflict markers ==="
	@if git grep -nE '^(<{7}|>{7})( |$$)' -- . ; then \
	    echo "conflict markers: FAIL"; exit 1; fi
	@echo "conflict markers: OK"

analyze: check-conflict-markers | $(BUILD)
	@echo "=== stack-usage ==="
	@mkdir -p $(BUILD)/analyze
	@for f in $(SRCS); do \
	    base=$$(basename $$f .c); \
	    $(CC) $(ANALYZE_CFLAGS) -fstack-usage -c $$f -o $(BUILD)/analyze/$$base.o 2>/dev/null; \
	done
	@for f in $(VIRT_SRCS); do \
	    base=$$(basename $$f .c); \
	    $(CC) $(ANALYZE_CFLAGS) $(VIRT_CFLAGS) -fstack-usage -c $$f -o $(BUILD)/analyze/$$base.o 2>/dev/null; \
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
	@for f in $(VIRT_SRCS); do \
	    $(CC) $(ANALYZE_CFLAGS) $(VIRT_CFLAGS) -fanalyzer -fsyntax-only $$f 2>&1; \
	done
	@echo "gcc-fanalyzer: OK"
	@echo "=== cppcheck ==="
	@cppcheck --enable=warning,performance,portability --error-exitcode=1 \
	    --suppress=missingIncludeSystem \
	    --suppress=normalCheckLevelMaxBranches \
	    --suppress=toomanyconfigs \
	    --suppress=*:$(CUTILS_DIR)/lib/cJSON/* \
	    --suppress=oppositeInnerCondition:src/store/object.c \
	    -DVERSION_STRING=\"0.0.0\" \
	    --inline-suppr \
	    --quiet $(SRC_INCLUDES) -DHAVE_LIBVIRT $(SRCS) $(VIRT_SRCS)
	@echo "cppcheck: OK"

lint:
	@echo "=== clang-tidy ==="
	@clang-tidy $(SRCS) -- -std=c11 -pthread $(SRC_INCLUDES) 2>&1 | \
	    grep -E "warning:|error:" || echo "clang-tidy: OK"

# ---- Editor index -----------------------------------------------------
#
# clangd reads compile_commands.json and nothing else, so a stale one is
# worse than none: it answers for files that no longer exist and falls back
# to guessed flags for the ones that do, which reports every include in the
# tree as missing. The one here predated the daemonization rewrite by five
# months and named src/xfer.c.
#
# Regenerate after adding a source file, moving one, or changing the include
# paths. bear observes a real build, so it has to be a build that actually
# runs the compiler - hence the clean.
.PHONY: compile-commands
compile-commands:
	@command -v bear >/dev/null || { echo "bear not installed"; exit 1; }
	rm -rf $(BUILD)
	bear --output compile_commands.json -- $(MAKE) -j8 \
	    all $(TEST_BINS) $(FAULT_BINS) fuzz
	@echo "compile_commands.json: $$(python3 -c 'import json;print(len(json.load(open("compile_commands.json"))))') entries"

# ---- Benchmarks -------------------------------------------------------
BENCH        := bench
# The daemon's include set and warning set. A private -I list here went stale
# twice: after the April src/ reorganisation, and again when the daemon
# rewrite added src/meta, src/scheduler, src/config and the c-utils headers.
BENCH_CFLAGS := -std=c11 -pthread -Wall -Wextra -Wpedantic $(WARN_FLAGS) -O3 -flto=auto $(ARCH_CFLAGS) \
                $(SRC_INCLUDES)

$(BINS)/bench_micro: $(BENCH)/micro.c $(REL_LIB_OBJS) | $(BINS)
	$(CC) $(BENCH_CFLAGS) $^ -o $@ -flto=auto $(LDFLAGS)

$(BINS)/bench_phases: $(BENCH)/phases.c $(BENCH)/bench_common.h $(REL_LIB_OBJS) | $(BINS)
	$(CC) $(BENCH_CFLAGS) $(filter %.c %.o,$^) -o $@ -flto=auto $(LDFLAGS)

$(BINS)/bench_browse: $(BENCH)/browse.c $(BENCH)/bench_common.h $(REL_LIB_OBJS) | $(BINS)
	$(CC) $(BENCH_CFLAGS) $(filter %.c %.o,$^) -o $@ -flto=auto $(LDFLAGS)

$(BINS)/bench_nbd: $(BENCH)/nbd.c $(REL_LIB_OBJS) | $(BINS)
	$(CC) $(BENCH_CFLAGS) $^ -o $@ -flto=auto $(LDFLAGS)

# NBD read throughput, depth 1 against 8, through qemu-nbd (PERF.md).
bench-nbd: $(BINS)/bench_nbd
	$(BINS)/bench_nbd

# An LD_PRELOAD shim, not linked into anything: it delays fsync and fdatasync
# under chosen directories so bench_phases can model slow sync storage. See
# bench/slow_sync.c.
$(BINS)/slow_sync.so: $(BENCH)/slow_sync.c | $(BINS)
	$(CC) -std=c11 -pthread -Wall -Wextra -Wpedantic $(WARN_FLAGS) -O2 -fPIC -shared $< -o $@ -ldl -pthread

# Another LD_PRELOAD shim: counts and times libc calls and samples the main
# thread's stack, for profiling where perf and strace are unavailable. See
# bench/prof_shim.c; bench/prof_report.py reads what it writes.
$(BINS)/prof_shim.so: $(BENCH)/prof_shim.c | $(BINS)
	$(CC) -std=c11 -pthread -Wall -Wextra -Wpedantic $(WARN_FLAGS) -O2 -fPIC -shared $< -o $@ -ldl -pthread -lrt

bench: $(BINS)/bench_micro $(BINS)/bench_phases $(BINS)/bench_browse $(BINS)/slow_sync.so
	@echo "=== micro ===" && $(BINS)/bench_micro
	@echo "=== phases ===" && $(BINS)/bench_phases
	@echo "=== browse ===" && $(BINS)/bench_browse

bench-micro: $(BINS)/bench_micro
	$(BINS)/bench_micro

bench-phases: $(BINS)/bench_phases
	$(BINS)/bench_phases

bench-browse: $(BINS)/bench_browse
	$(BINS)/bench_browse

# ---- Coverage ---------------------------------------------------------
COV_DIR     := $(BUILD)/coverage
# -fprofile-update=atomic: the store pool, the pack workers and the job
# manager run the same instrumented blocks on several threads at once, and
# gcov's default counter update is a plain load-add-store. Concurrent
# increments are lost, the arcs of a block stop adding up, and the counts
# gcov derives from them by subtraction come out negative: "branch 1 taken
# -1" in store_pool.c, which gcovr rejects, intermittently failing the CI
# coverage job (FLAKE-9). With the default, eight threads in one loop kept
# 14% of their increments. gcc picks atomic updates by itself under
# -pthread, which the build now uses too, but the flag stays explicit so
# coverage never depends on how the link line is spelled.
COV_PROFILE := -fprofile-update=atomic
COV_CFLAGS  := -std=c11 -pthread -Wall -Wextra -O0 -g --coverage $(COV_PROFILE) -march=$(MARCH) \
               -Wno-unused-result -Wno-format-truncation $(SRC_INCLUDES) \
               $(VERSION_DEF) $(CUTILS_VERSION_DEF)
COV_LDFLAGS := --coverage $(LDFLAGS)
COV_OBJS    := $(patsubst $(SRC)/%.c, $(COV_DIR)/%.o, $(SRCS))
COV_OBJS    += $(COV_DIR)/app_migrations.o
COV_MAIN    := $(COV_DIR)/main.o
COV_LIB     := $(filter-out $(COV_MAIN), $(COV_OBJS))
COV_TESTS   := $(patsubst $(TESTS)/%.c, $(COV_DIR)/%, $(filter-out $(wildcard $(TESTS)/test_virt*.c), $(TEST_SRCS)))
# The fault suites too. They were left out because TEST_SRCS filters them -
# it feeds both TEST_BINS and COV_TESTS, and they need their own link rule
# for --wrap - so nineteen tests that exercise nothing but error paths in
# object.c and pack.c ran on every push and counted for nothing. Those are
# the same error paths this file's ratchet comment calls the gap to 80%.
COV_FAULT_TESTS := $(patsubst $(TESTS)/%.c, $(COV_DIR)/%, $(FAULT_SRCS))
# The daemon and CLI themselves, instrumented, so the shell suites count. They
# are the only tests that reach the routes, the auth middleware, the job
# manager and both main()s, and without them every line those cover reported
# as untested: the whole-repo figure read 60% while the code was exercised on
# every push. gcov writes its counters at exit, which works because both
# suites stop the daemon with SIGTERM and wait for it.
COV_DAEMON := $(COV_DIR)/bins/c-backupd
COV_CLI    := $(COV_DIR)/bins/c-backup

$(COV_DIR):
	mkdir -p $(COV_DIR)

COV_STAMP := $(COV_DIR)/flags.stamp
COV_SIG   := $(call flag_sig,$(COV_CFLAGS) $(COV_LDFLAGS))
ifneq ($(filter coverage $(COV_DIR)/%,$(MAKECMDGOALS)),)
$(eval $(call track_flags,$(COV_DIR),COV_SIG))
endif
$(eval $(call stamp_rule,$(COV_DIR),COV_SIG))

$(COV_DIR)/%.o: $(SRC)/%.c $(COV_STAMP) | $(COV_DIR)
	@mkdir -p $(dir $@)
	$(CC) $(COV_CFLAGS) -MMD -MP -c $< -o $@

$(COV_DIR)/app_migrations.o: $(MIGRATE_SRC) $(COV_STAMP) | $(COV_DIR)
	$(CC) -std=c11 -pthread -O0 -g --coverage $(COV_PROFILE) $(SRC_INCLUDES) -c $< -o $@

$(COV_DIR)/%: $(TESTS)/%.c $(TESTS)/fake_index.c $(COV_LIB) $(TEST_HDRS) $(TEST_DIR_STAMP) | $(COV_DIR)
	$(CC) $(COV_CFLAGS) $(TEST_DIR_DEF) $(filter %.c %.o,$^) -o $@ $(COV_LDFLAGS) -lcmocka

$(COV_DIR)/daemon_main.o: $(DAEMON_MAIN) $(COV_STAMP) | $(COV_DIR)
	$(CC) $(COV_CFLAGS) -MMD -MP -c $< -o $@
$(COV_DIR)/cli_main.o: $(CLI_MAIN) $(COV_STAMP) | $(COV_DIR)
	$(CC) $(COV_CFLAGS) -MMD -MP -c $< -o $@

-include $(shell find $(COV_DIR) -name '*.d' 2>/dev/null)

$(COV_DAEMON): $(COV_DIR)/daemon_main.o $(COV_LIB)
	@mkdir -p $(dir $@)
	$(CC) $(COV_CFLAGS) $^ -o $@ $(COV_LDFLAGS)
$(COV_CLI): $(COV_DIR)/cli_main.o $(COV_LIB)
	@mkdir -p $(dir $@)
	$(CC) $(COV_CFLAGS) $^ -o $@ $(COV_LDFLAGS)

# Shorter stem, so make prefers this over the rule above for the fault
# suites - the same way the release build already resolves them.
$(COV_DIR)/test_%_fault: $(TESTS)/test_%_fault.c $(TESTS)/fault_inject.c $(TESTS)/fake_index.c $(COV_LIB) $(TEST_HDRS) $(TEST_DIR_STAMP) | $(COV_DIR)
	$(CC) $(COV_CFLAGS) $(TEST_DIR_DEF) $(filter %.c %.o,$^) -o $@ $(COV_LDFLAGS) $(FAULT_WRAP) -lcmocka

# Two gates. The whole repository holds the 80% standard. src/store/ has a
# ratchet of its own because it is the code that survives: the floor is what
# it actually achieves, raised as tests land, and the standard is 80% there too.
#
# What stands between src/store and 80%, measured rather than guessed:
#
#   - pack.c's largest block is the RS repair inside pack_object_load_stream.
#     A flipped payload byte does not reach it: the block-level parity repairs
#     the damage before the CRC scan runs, so need_rs_repair never sets. It
#     needs corruption crafted to survive that earlier layer.
#   - The rest is error paths behind read(), write() and pwrite(), which the
#     fault injector does not wrap yet.
#
# The FUSE readahead ring used to be the largest single block. It could not be
# switched on from any configuration and was removed instead of tested.
COV_MIN       := 80
COV_STORE_MIN := 78

# The suites run with their output discarded, since `make test` is where they
# are read. A hang there was invisible: a CI coverage job sat for 42 minutes
# with nothing in its log after the build. Each suite gets a deadline, and
# one that fails or overruns is named, without failing the target - the
# coverage gates decide that.
COV_TEST_TIMEOUT := 300
COV_E2E_TIMEOUT  := 900

# gcovr rejects any count over 2^32 as "suspicious", a guard against gcc bug
# 68080: racing non-atomic counter updates in threaded code produced bogus
# counts. COV_PROFILE makes the updates atomic, so the counts are real, and
# one is legitimately far past 2^32: with no AVX2 (CI builds x86-64-v2),
# rs_decode's scalar syndrome loop calls gf_mul about 31 billion times in a
# coverage run. The racy counters used to lose most of those increments,
# which is the only reason CI stayed under the line. 2^40 still catches a
# counter that wrapped or was garbage, which reads near 2^64.
#
# The check and its flag arrived in gcovr 8; Debian trixie ships 7.2, which
# has neither and refuses the unknown flag, so pass it only when the
# installed gcovr knows it.
GCOVR_FLAGS := $(shell gcovr --help 2>/dev/null | grep -q -- --gcov-suspicious-hits-threshold \
                 && echo --gcov-suspicious-hits-threshold 1099511627776)

# The whole-repo summary prints last, on purpose. GitLab's coverage regex keeps
# the last "lines:" in the log, and printing the store gate second made every
# pipeline report src/store as the project figure. The HTML report is still
# written first, so a failing store gate leaves it behind for the artifact.
#
# Counters from an earlier run are cleared first. gcov adds to whatever .gcda
# it finds, so a second local run reported the sum of both, and a few runs in
# the hit counts on parity.c's SIMD loop grew past what gcovr will believe.
#
# As root (CI), the suites that leave root still count. The helper suites
# drop to a fixed non-root uid for good (helper_fixture.h hf_unroot), and
# gcov writes their counters at exit, as that uid, into .gcda files the
# root-run suites before them had already created 0644 and root's: every
# write failed, and helper_server.c, helper_proto.c, helper_policy.c and
# remote_provider.c read 0% while their suites ran and passed (pipeline
# 5214: 79.3% against 80%; 86.8% measured as a normal user). So, as root
# only, the coverage tree's directories take any writer and the suites run
# under umask 000, which makes the counter files gcov creates 0666.
coverage: $(COV_TESTS) $(COV_FAULT_TESTS) $(COV_DAEMON) $(COV_CLI) $(FREE_PORT)
	@find $(COV_DIR) -name '*.gcda' -delete
	@if [ "$$(id -u)" = 0 ]; then find $(COV_DIR) -type d -exec chmod a+rwx {} +; fi
	@if [ "$$(id -u)" = 0 ]; then umask 000; fi; \
	over=0; \
	run() { n=$$1; shift; \
	    C_BACKUP_SCRATCH_QUIET=1 $(SCRATCH_GUARD) "$$n" "$$@"; rc=$$?; \
	    [ $$rc -eq 99 ] && over=1; \
	    [ $$rc -eq 0 ] || echo "coverage: $$n failed or timed out"; }; \
	for t in $(COV_TESTS) $(COV_FAULT_TESTS); do \
	    run $$(basename $$t) timeout $(COV_TEST_TIMEOUT) $$t; \
	done; \
	run test_api_e2e.sh timeout $(COV_E2E_TIMEOUT) ./$(TESTS)/test_api_e2e.sh $(COV_DAEMON); \
	run test_cli_e2e.sh timeout $(COV_E2E_TIMEOUT) ./$(TESTS)/test_cli_e2e.sh $(COV_DAEMON) $(COV_CLI); \
	run test_units_e2e.sh timeout $(COV_E2E_TIMEOUT) ./$(TESTS)/test_units_e2e.sh $(COV_DAEMON) $(COV_CLI); \
	if [ $$over -ne 0 ]; then echo "coverage: a suite went over its scratch budget" >&2; exit 1; fi
	@gcovr $(GCOVR_FLAGS) -r $(SRC) $(COV_DIR) --html-details $(COV_DIR)/coverage.html
	@echo "=== src/store gate (min $(COV_STORE_MIN)% lines) ==="
	@gcovr $(GCOVR_FLAGS) -r $(SRC) $(COV_DIR) --filter 'src/store/.*' \
	    --fail-under-line $(COV_STORE_MIN) -s
	@echo "=== whole repo (min $(COV_MIN)% lines) ==="
	@gcovr $(GCOVR_FLAGS) -r $(SRC) $(COV_DIR) --txt $(COV_DIR)/coverage.txt \
	    --fail-under-line $(COV_MIN) -s
	@echo "Coverage report: $(COV_DIR)/coverage.html"

.PHONY: coverage

# ---- Clean ------------------------------------------------------------
clean:
	rm -rf $(BUILD)
