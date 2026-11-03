CXX ?= g++
CXXFLAGS ?= -std=c++20 -O3 -msse4.2 -Wall -Wextra -Wpedantic -Iinclude

ifeq ($(OS),Windows_NT)
    TARGET_MAIN = g12_sequencer.exe
    TARGET_TEST = test_sequencer.exe
    LDFLAGS = 
else
    TARGET_MAIN = g12_sequencer
    TARGET_TEST = test_sequencer
    LDFLAGS = -pthread -lrt
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
