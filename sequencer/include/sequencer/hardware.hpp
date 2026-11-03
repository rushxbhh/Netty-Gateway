#pragma once

#include <cstdint>
#include <cstddef>
#include <chrono>

#if defined(_WIN32) || defined(_WIN64)
    #ifndef NOMINMAX
        #define NOMINMAX 1
    #endif
    #include <windows.h>
#else
    #include <pthread.h>
    #include <sched.h>
#endif

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
    #include <immintrin.h>
    #include <x86intrin.h>
    #define G12_HAS_X86_INTRINSICS 1
#endif

namespace g12::hw {

/**
 * High-resolution hardware timestamp using CPU Cycle Counter (RDTSC).
 * Provides sub-nanosecond resolution for latency profiling and deterministic timestamping.
 */
[[nodiscard]] inline uint64_t rdtsc() noexcept {
#if defined(G12_HAS_X86_INTRINSICS)
    return __rdtsc();
#else
    // Fallback using steady_clock nanoseconds
    return static_cast<uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count()
    );
#endif
}

/**
 * Serializing variant of RDTSC that prevents out-of-order execution before reading.
 */
[[nodiscard]] inline uint64_t rdtscp() noexcept {
#if defined(G12_HAS_X86_INTRINSICS)
    unsigned int aux = 0;
    return __rdtscp(&aux);
#else
    return rdtsc();
#endif
}

/**
 * Hint to CPU that thread is in a spin-wait loop.
 * Reduces power consumption and avoids pipeline stall hazards upon loop exit.
 */
inline void spin_pause() noexcept {
#if defined(G12_HAS_X86_INTRINSICS)
    _mm_pause();
#elif defined(_WIN32) || defined(_WIN64)
    YieldProcessor();
#else
    #if defined(__has_builtin) && __has_builtin(__builtin_ia32_pause)
        __builtin_ia32_pause();
    #else
        // No-op fallback
    #endif
#endif
}

/**
 * Software CRC32C (Castagnoli 0x82F63B78) portable implementation.
 */
inline uint32_t crc32c_software(const void* data, size_t length) noexcept {
    static constexpr uint32_t poly = 0x82F63B78;
    uint32_t crc = 0xFFFFFFFF;
    const auto* p = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < length; ++i) {
        crc ^= p[i];
        for (int k = 0; k < 8; ++k) {
            crc = (crc >> 1) ^ ((crc & 1) ? poly : 0);
        }
    }
    return crc ^ 0xFFFFFFFF;
}

/**
 * Hardware-accelerated CRC32C calculation using SSE4.2 instructions (_mm_crc32_u64 / _mm_crc32_u8).
 * Falls back to software Castagnoli implementation on non-x86.
 */
[[nodiscard]] inline uint32_t crc32c(const void* data, size_t length) noexcept {
#if defined(G12_HAS_X86_INTRINSICS) && (defined(__SSE4_2__) || defined(_M_X64))
    const auto* ptr = static_cast<const uint8_t*>(data);
    uint64_t crc = 0xFFFFFFFF;

    // Process 8-byte chunks
    while (length >= 8) {
        uint64_t val = 0;
        __builtin_memcpy(&val, ptr, 8);
        crc = _mm_crc32_u64(crc, val);
        ptr += 8;
        length -= 8;
    }

    // Process remaining bytes
    uint32_t crc32 = static_cast<uint32_t>(crc);
    while (length > 0) {
        crc32 = _mm_crc32_u8(crc32, *ptr);
        ptr++;
        length--;
    }
    return crc32 ^ 0xFFFFFFFF;
#else
    return crc32c_software(data, length);
#endif
}

/**
 * Pins the calling thread to a specific CPU core.
 * Eliminates OS context switching overhead and preserves L1/L2 CPU cache locality.
 */
inline bool set_thread_affinity(int core_id) noexcept {
    if (core_id < 0) return false;

#if defined(_WIN32) || defined(_WIN64)
    if (core_id >= 64) return false;
    DWORD_PTR mask = static_cast<DWORD_PTR>(1ULL << core_id);
    return SetThreadAffinityMask(GetCurrentThread(), mask) != 0;
#else
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    return pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) == 0;
#endif
}

} // namespace g12::hw
