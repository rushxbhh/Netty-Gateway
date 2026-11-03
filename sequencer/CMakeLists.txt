cmake_minimum_required(VERSION 3.20)
project(g12_sequencer VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Optimization and warning flags for ultra-low latency hot path
if(MSVC)
    add_compile_options(/O2 /Oi /W4)
else()
    add_compile_options(-O3 -march=native -msse4.2 -Wall -Wextra -Wpedantic)
endif()

include_directories(include)

# 1. Main Sequencer Daemon
add_executable(g12_sequencer src/main.cpp)

# 2. Automated Test Suite & Benchmark
add_executable(test_sequencer tests/test_sequencer.cpp)

# Threads linkage on Linux
find_package(Threads REQUIRED)
target_link_libraries(g12_sequencer PRIVATE Threads::Threads)
target_link_libraries(test_sequencer PRIVATE Threads::Threads)

if(NOT WIN32)
    # POSIX Real-time library (for shm_open / clock_gettime)
    target_link_libraries(g12_sequencer PRIVATE rt)
    target_link_libraries(test_sequencer PRIVATE rt)
endif()
