#pragma once

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <atomic>
#include <string>
#include <stdexcept>
#include <thread>
#include <chrono>

#include "ring_layout.hpp"
#include "hardware.hpp"

#if defined(_WIN32) || defined(_WIN64)
    #ifndef NOMINMAX
        #define NOMINMAX 1
    #endif
    #include <windows.h>
#else
    #include <sys/mman.h>
    #include <sys/stat.h>
    #include <fcntl.h>
    #include <unistd.h>
#endif

namespace g12 {

// Ensure 64-bit atomics are truly lock-free on this platform
static_assert(std::atomic<uint64_t>::is_always_lock_free, "std::atomic<uint64_t> must be lock-free");

/**
 * Cross-platform memory-mapped file abstraction.
 */
class MmapFile {
public:
    MmapFile() = default;
    ~MmapFile() { close(); }

    // Non-copyable
    MmapFile(const MmapFile&) = delete;
    MmapFile& operator=(const MmapFile&) = delete;

    // Movable
    MmapFile(MmapFile&& other) noexcept { move_from(std::move(other)); }
    MmapFile& operator=(MmapFile&& other) noexcept {
        if (this != &other) {
            close();
            move_from(std::move(other));
        }
        return *this;
    }

    /**
     * Maps an existing file or creates one with the required size.
     */
    bool open_or_create(const std::string& path, size_t required_size, bool create_if_missing = true) {
        close();
        path_ = path;
        size_ = required_size;

#if defined(_WIN32) || defined(_WIN64)
        DWORD creation = create_if_missing ? OPEN_ALWAYS : OPEN_EXISTING;
        file_handle_ = CreateFileA(
            path.c_str(),
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            nullptr,
            creation,
            FILE_ATTRIBUTE_NORMAL,
            nullptr
        );

        if (file_handle_ == INVALID_HANDLE_VALUE) {
            return false;
        }

        LARGE_INTEGER cur_size;
        if (GetFileSizeEx(file_handle_, &cur_size)) {
            if (static_cast<size_t>(cur_size.QuadPart) < required_size) {
                LARGE_INTEGER li_size;
                li_size.QuadPart = static_cast<LONGLONG>(required_size);
                SetFilePointerEx(file_handle_, li_size, nullptr, FILE_BEGIN);
                SetEndOfFile(file_handle_);
            }
        }

        map_handle_ = CreateFileMappingA(
            file_handle_,
            nullptr,
            PAGE_READWRITE,
            0,
            0,
            nullptr
        );

        if (map_handle_ == nullptr) {
            CloseHandle(file_handle_);
            file_handle_ = INVALID_HANDLE_VALUE;
            return false;
        }

        mapped_addr_ = MapViewOfFile(
            map_handle_,
            FILE_MAP_ALL_ACCESS,
            0,
            0,
            required_size
        );

        if (mapped_addr_ == nullptr) {
            CloseHandle(map_handle_);
            CloseHandle(file_handle_);
            map_handle_ = nullptr;
            file_handle_ = INVALID_HANDLE_VALUE;
            return false;
        }
#else
        int flags = O_RDWR;
        if (create_if_missing) flags |= O_CREAT;
        fd_ = ::open(path.c_str(), flags, 0666);
        if (fd_ < 0) {
            return false;
        }

        struct stat st{};
        if (fstat(fd_, &st) == 0) {
            if (static_cast<size_t>(st.st_size) < required_size) {
                if (ftruncate(fd_, static_cast<off_t>(required_size)) != 0) {
                    ::close(fd_);
                    fd_ = -1;
                    return false;
                }
            }
        }

        mapped_addr_ = mmap(nullptr, required_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (mapped_addr_ == MAP_FAILED) {
            mapped_addr_ = nullptr;
            ::close(fd_);
            fd_ = -1;
            return false;
        }
#endif
        return true;
    }

    void close() noexcept {
#if defined(_WIN32) || defined(_WIN64)
        if (mapped_addr_) {
            UnmapViewOfFile(mapped_addr_);
            mapped_addr_ = nullptr;
        }
        if (map_handle_) {
            CloseHandle(map_handle_);
            map_handle_ = nullptr;
        }
        if (file_handle_ != INVALID_HANDLE_VALUE) {
            CloseHandle(file_handle_);
            file_handle_ = INVALID_HANDLE_VALUE;
        }
#else
        if (mapped_addr_) {
            munmap(mapped_addr_, size_);
            mapped_addr_ = nullptr;
        }
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
#endif
        size_ = 0;
    }

    [[nodiscard]] void* data() const noexcept { return mapped_addr_; }
    [[nodiscard]] size_t size() const noexcept { return size_; }
    [[nodiscard]] bool is_open() const noexcept { return mapped_addr_ != nullptr; }

private:
    void move_from(MmapFile&& other) noexcept {
        path_ = std::move(other.path_);
        size_ = other.size_;
        mapped_addr_ = other.mapped_addr_;
#if defined(_WIN32) || defined(_WIN64)
        file_handle_ = other.file_handle_;
        map_handle_ = other.map_handle_;
        other.file_handle_ = INVALID_HANDLE_VALUE;
        other.map_handle_ = nullptr;
#else
        fd_ = other.fd_;
        other.fd_ = -1;
#endif
        other.mapped_addr_ = nullptr;
        other.size_ = 0;
    }

    std::string path_;
    size_t size_ = 0;
    void* mapped_addr_ = nullptr;

#if defined(_WIN32) || defined(_WIN64)
    HANDLE file_handle_ = INVALID_HANDLE_VALUE;
    HANDLE map_handle_ = nullptr;
#else
    int fd_ = -1;
#endif
};

/**
 * Single-Producer Single-Consumer (SPSC) Lock-Free Ring Buffer.
 */
class RingBuffer {
public:
    RingBuffer() = default;

    /**
     * Initializes the ring file header and counters.
     * Used when C++ acts as creator, or in tests.
     */
    static bool initialize_ring(
        void* base_addr,
        uint32_t ring_kind,
        uint64_t capacity,
        uint32_t slot_size
    ) noexcept {
        if (!base_addr) return false;
        auto* base = static_cast<uint8_t*>(base_addr);

        // Zero out headers & counters
        std::memset(base, 0, OFFSET_SLOTS_START);

        // 1. Initialize Header Line (offset 0)
        auto* header = reinterpret_cast<RingHeaderLine*>(base + OFFSET_HEADER_LINE);
        header->version = RING_VERSION;
        header->slotSize = slot_size;
        header->capacity = capacity;
        header->ringKind = ring_kind;

        // Initialize counters to 0
        auto* producer_seq = reinterpret_cast<std::atomic<uint64_t>*>(base + OFFSET_PRODUCER_SEQ);
        auto* consumer_seq = reinterpret_cast<std::atomic<uint64_t>*>(base + OFFSET_CONSUMER_SEQ);
        producer_seq->store(0, std::memory_order_relaxed);
        consumer_seq->store(0, std::memory_order_relaxed);

        // Magic bytes written LAST with release barrier as per specification
        std::atomic_thread_fence(std::memory_order_release);
        std::memcpy(header->magic, RING_MAGIC, sizeof(RING_MAGIC));
        return true;
    }

    /**
     * Attaches to a mapped ring buffer.
     * Waits until magic bytes "G12RING1" are written by Java if timeout > 0.
     */
    bool attach(
        void* base_addr,
        uint32_t expected_ring_kind,
        uint64_t expected_capacity,
        uint32_t expected_slot_size,
        std::chrono::milliseconds wait_magic_timeout = std::chrono::milliseconds(5000)
    ) {
        if (!base_addr) return false;
        base_ = static_cast<uint8_t*>(base_addr);

        auto* header = reinterpret_cast<const RingHeaderLine*>(base_ + OFFSET_HEADER_LINE);
        auto start_wait = std::chrono::steady_clock::now();

        // Spin until magic is valid or timeout occurs
        while (std::memcmp(header->magic, RING_MAGIC, sizeof(RING_MAGIC)) != 0) {
            if (std::chrono::steady_clock::now() - start_wait > wait_magic_timeout) {
                return false;
            }
            hw::spin_pause();
        }

        capacity_ = header->capacity;
        slot_size_ = header->slotSize;
        ring_kind_ = header->ringKind;

        if (ring_kind_ != expected_ring_kind ||
            capacity_ != expected_capacity ||
            slot_size_ != expected_slot_size) {
            return false;
        }

        producer_seq_ = reinterpret_cast<std::atomic<uint64_t>*>(base_ + OFFSET_PRODUCER_SEQ);
        consumer_seq_ = reinterpret_cast<std::atomic<uint64_t>*>(base_ + OFFSET_CONSUMER_SEQ);
        return true;
    }

    [[nodiscard]] uint64_t capacity() const noexcept { return capacity_; }
    [[nodiscard]] size_t slot_size() const noexcept { return slot_size_; }

    // Direct atomic pointers for SPSC protocol
    [[nodiscard]] std::atomic<uint64_t>& producer_seq() noexcept { return *producer_seq_; }
    [[nodiscard]] std::atomic<uint64_t>& consumer_seq() noexcept { return *consumer_seq_; }
    [[nodiscard]] const std::atomic<uint64_t>& producer_seq() const noexcept { return *producer_seq_; }
    [[nodiscard]] const std::atomic<uint64_t>& consumer_seq() const noexcept { return *consumer_seq_; }

    /**
     * Returns pointer to the slot memory for a given sequence number.
     */
    [[nodiscard]] uint8_t* slot_ptr(uint64_t seq) noexcept {
        return base_ + OFFSET_SLOTS_START + (seq & (capacity_ - 1)) * slot_size_;
    }

    [[nodiscard]] const uint8_t* slot_ptr(uint64_t seq) const noexcept {
        return base_ + OFFSET_SLOTS_START + (seq & (capacity_ - 1)) * slot_size_;
    }

private:
    uint8_t* base_ = nullptr;
    uint64_t capacity_ = 0;
    size_t   slot_size_ = 0;
    uint32_t ring_kind_ = 0;

    std::atomic<uint64_t>* producer_seq_ = nullptr;
    std::atomic<uint64_t>* consumer_seq_ = nullptr;
};

} // namespace g12
