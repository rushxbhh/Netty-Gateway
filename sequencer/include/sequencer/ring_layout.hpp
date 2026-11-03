#pragma once

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <type_traits>

namespace g12 {

// Magic bytes: ASCII "G12RING1"
inline constexpr char RING_MAGIC[8] = {'G', '1', '2', 'R', 'I', 'N', 'G', '1'};
inline constexpr uint32_t RING_VERSION = 1;

// Ring kinds
inline constexpr uint32_t RING_KIND_INBOUND = 1;
inline constexpr uint32_t RING_KIND_JOURNAL = 2;

// Offset constants
inline constexpr size_t OFFSET_HEADER_LINE  = 0;
inline constexpr size_t OFFSET_PRODUCER_SEQ = 64;
inline constexpr size_t OFFSET_CONSUMER_SEQ = 128;
inline constexpr size_t OFFSET_SLOTS_START  = 192;

// Default provisional capacities & slot sizes
inline constexpr size_t INBOUND_SLOT_SIZE    = 128;
inline constexpr size_t INBOUND_CAPACITY     = 65536;    // 64K slots (~8MB file)
inline constexpr size_t INBOUND_PAYLOAD_MAX  = 96;

inline constexpr size_t JOURNAL_SLOT_SIZE    = 64;
inline constexpr size_t JOURNAL_CAPACITY     = 1048576;  // 1M slots (~64MB file)

// Transaction Types (matching transaction.fbs: TRANSFER=0, DEPOSIT=1, WITHDRAWAL=2)
enum class TxnType : uint8_t {
    TRANSFER   = 0,
    DEPOSIT    = 1,
    WITHDRAWAL = 2
};

// Journal Status
enum class JournalStatus : uint8_t {
    SEQUENCED_OK       = 0,
    REJECTED_MALFORMED = 1
};

#pragma pack(push, 1)

/**
 * 64-byte Header Line at offset 0 of every ring file.
 */
struct RingHeaderLine {
    char     magic[8];       // "G12RING1" (written LAST with release store)
    uint32_t version;        // 1
    uint32_t slotSize;       // Multiple of 64
    uint64_t capacity;       // Power of two
    uint32_t ringKind;       // 1 = inbound, 2 = journal
    uint8_t  reserved[36];   // Zeroed
};
static_assert(sizeof(RingHeaderLine) == 64, "RingHeaderLine must be exactly 64 bytes");

/**
 * 128-byte Inbound Slot format (offset 192 + seq * 128).
 */
struct alignas(64) InboundSlot {
    uint32_t payloadLen;     // Length of FlatBuffer bytes (must be <= 96)
    uint32_t flags;          // Reserved, 0
    uint64_t connectionId;   // Connection ID assigned by Java Netty Gateway
    uint8_t  reserved[16];   // Zero
    uint8_t  payload[96];    // Raw FlatBuffer binary payload
};
static_assert(sizeof(InboundSlot) == 128, "InboundSlot must be exactly 128 bytes");

/**
 * Exactly 64-byte Journal Slot format (offset 192 + seq * 64).
 * Single CPU cache-line aligned (alignas(64)) to prevent false sharing.
 */
struct alignas(64) JournalSlot {
    uint64_t sequenceId;     // Assigned by sequencer, equals journal slot's monotonic seq
    uint64_t timestampTsc;   // Hardware RDTSC timestamp
    uint64_t clientTxnId;    // Idempotency key from client FlatBuffer
    uint64_t connectionId;   // Copied from Inbound slot
    uint64_t fromAccount;    // Sender account ID (0 = system)
    uint64_t toAccount;      // Receiver account ID (0 = system)
    int64_t  amount;         // Smallest currency unit (cents/paise)
    uint8_t  type;           // 0=TRANSFER, 1=DEPOSIT, 2=WITHDRAWAL
    uint8_t  status;         // 0=SEQUENCED_OK, 1=REJECTED_MALFORMED
    uint16_t reserved;       // Zero
    uint32_t checksum;       // CRC32C over bytes 0..59
};
static_assert(sizeof(JournalSlot) == 64, "JournalSlot must be exactly 64 bytes");

#pragma pack(pop)

/**
 * Calculates the exact file offset for a given sequence number.
 */
[[nodiscard]] inline constexpr size_t slot_file_offset(uint64_t seq, uint64_t capacity, size_t slotSize) noexcept {
    return OFFSET_SLOTS_START + (seq & (capacity - 1)) * slotSize;
}

/**
 * Calculates the total file size required for a ring buffer.
 */
[[nodiscard]] inline constexpr size_t ring_total_file_size(uint64_t capacity, size_t slotSize) noexcept {
    return OFFSET_SLOTS_START + capacity * slotSize;
}

} // namespace g12
