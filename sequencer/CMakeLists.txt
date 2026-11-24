cmake_minimum_required(VERSION 3.20)
project(g12_sequencer VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Optimization and warning flags for ultra-low latency hot path
if(MSVC)
    add_compile_options(/O2 /Oi /W4)
else()
    add_compile_options(-O3 -Wall -Wextra -Wpedantic)

    # SSE4.2 CRC32C is only available on x86. ARM64 builds (including Apple
    # Silicon Docker images) use the portable software CRC32C fallback.
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64|i.86")
        include(CheckCXXCompilerFlag)
        check_cxx_compiler_flag("-msse4.2" G12_HAS_MSSE42)
        if(G12_HAS_MSSE42)
            add_compile_options(-msse4.2)
        endif()
    endif()
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

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    # POSIX Real-time library (not required on macOS; included on Linux).
    target_link_libraries(g12_sequencer PRIVATE rt)
    target_link_libraries(test_sequencer PRIVATE rt)
endif()
