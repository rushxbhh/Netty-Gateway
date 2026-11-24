CXX ?= g++
HOST_ARCH := $(shell uname -m 2>/dev/null || echo unknown)
ifneq ($(filter x86_64 amd64 i386 i486 i586 i686,$(HOST_ARCH)),)
    SIMD_FLAGS := -msse4.2
else
    SIMD_FLAGS :=
endif
CXXFLAGS ?= -std=c++20 -O3 $(SIMD_FLAGS) -Wall -Wextra -Wpedantic -Iinclude

ifeq ($(OS),Windows_NT)
    TARGET_MAIN = g12_sequencer.exe
    TARGET_TEST = test_sequencer.exe
    LDFLAGS =
else
    TARGET_MAIN = g12_sequencer
    TARGET_TEST = test_sequencer
    UNAME_S := $(shell uname -s 2>/dev/null || echo unknown)
    ifeq ($(UNAME_S),Linux)
        LDFLAGS = -pthread -lrt
    else
        LDFLAGS = -pthread
    endif
endif

all: $(TARGET_MAIN) $(TARGET_TEST)

$(TARGET_MAIN): src/main.cpp
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

$(TARGET_TEST): tests/test_sequencer.cpp
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

test: $(TARGET_TEST)
	./$(TARGET_TEST)

clean:
	rm -f $(TARGET_MAIN) $(TARGET_TEST) *.ring *.o

.PHONY: all test clean
