CC      := gcc
CFLAGS  := -std=c11 -Wall -Wextra -Wpedantic -O2 \
           -I src -I vendor
LDFLAGS := -llz4 -lssl -lcrypto -lacl -lpthread

BUILD   := build
SRC     := src
TESTS   := tests

SRCS    := $(wildcard $(SRC)/*.c)
OBJS    := $(patsubst $(SRC)/%.c, $(BUILD)/%.o, $(SRCS))
MAIN_OBJ := $(BUILD)/main.o
LIB_OBJS := $(filter-out $(MAIN_OBJ), $(OBJS))

TARGET  := $(BUILD)/backup

TEST_SRCS := $(wildcard $(TESTS)/*.c)
TEST_BINS := $(patsubst $(TESTS)/%.c, $(BUILD)/%, $(TEST_SRCS))

.PHONY: all clean test

all: $(TARGET)

$(BUILD):
	mkdir -p $(BUILD)

$(BUILD)/%.o: $(SRC)/%.c | $(BUILD)
	$(CC) $(CFLAGS) -MMD -MP -c $< -o $@

-include $(OBJS:.o=.d)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

# Test binaries link lib objects (not main.o) + cmocka
$(BUILD)/%: $(TESTS)/%.c $(LIB_OBJS) | $(BUILD)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS) -lcmocka

test: $(TEST_BINS)
	@for t in $(TEST_BINS); do echo "=== $$t ==="; $$t; done

clean:
	rm -rf $(BUILD)
