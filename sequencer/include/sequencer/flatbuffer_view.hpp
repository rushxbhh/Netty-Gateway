#pragma once

#include <cstdint>
#include <cstddef>
#include <cstring>
#include "ring_layout.hpp"

namespace g12 {

/**
 * Parsed transaction payload extracted directly from FlatBuffer bytes.
 */
struct ParsedTransaction {
    uint64_t transactionId = 0;
    int64_t  amount = 0;
    uint64_t fromAccount = 0;
    uint64_t toAccount = 0;
    uint8_t  type = 0;
};

/**
 * Zero-copy, zero-allocation reader for TransactionMessage FlatBuffers.
 * Strictly checks bounds to ensure no invalid memory access occurs on corrupted frames.
 * File identifier: "NIGT"
 */
class FlatBufferView {
public:
    static constexpr size_t MIN_FLATBUFFER_SIZE = 8; // 4-byte root offset + 4-byte identifier
    static constexpr char IDENTIFIER[4] = {'N', 'I', 'G', 'T'};

    /**
     * Attempts to parse raw FlatBuffer bytes into a ParsedTransaction.
     * Returns true on success; false if buffer is malformed or out of bounds.
     */
    [[nodiscard]] static bool try_parse(
        const uint8_t* buffer,
        size_t size,
        ParsedTransaction& out
    ) noexcept {
        if (buffer == nullptr || size < MIN_FLATBUFFER_SIZE || size > INBOUND_PAYLOAD_MAX) {
            return false;
        }

        // 1. Verify 4-byte file identifier at offset 4
        if (buffer[4] != IDENTIFIER[0] ||
            buffer[5] != IDENTIFIER[1] ||
            buffer[6] != IDENTIFIER[2] ||
            buffer[7] != IDENTIFIER[3]) {
            return false;
        }

        // 2. Read root table offset (little-endian uint32_t). The root table
        // must begin after the root offset and 4-byte file identifier.
        uint32_t rootOffset = 0;
        std::memcpy(&rootOffset, buffer, sizeof(uint32_t));
        if (rootOffset < MIN_FLATBUFFER_SIZE || rootOffset > size - sizeof(int32_t)) {
            return false;
        }

        // 3. Read vtable offset (soffset_t, signed 32-bit). A deduplicated
        // vtable may be before or after the table, so keep this signed.
        int32_t vtableOffset = 0;
        std::memcpy(&vtableOffset, buffer + rootOffset, sizeof(int32_t));

        // In FlatBuffers, vtable position is (rootOffset - vtableOffset).
        const int64_t vtablePos = static_cast<int64_t>(rootOffset) - vtableOffset;
        if (vtablePos < 0 || vtablePos > static_cast<int64_t>(size)
                || static_cast<size_t>(vtablePos) > size - 6) {
            return false;
        }

        // 4. Read vtable header: vtableSize and the table's declared objectSize.
        uint16_t vtableSize = 0;
        uint16_t objectSize = 0;
        std::memcpy(&vtableSize, buffer + vtablePos, sizeof(uint16_t));
        std::memcpy(&objectSize, buffer + vtablePos + 2, sizeof(uint16_t));

        if (vtableSize < 6
                || vtableSize > size - static_cast<size_t>(vtablePos)
                || objectSize < sizeof(int32_t)
                || objectSize > size - rootOffset) {
            return false;
        }

        // 5. Field 0 (TransactionData struct) is at vtable offset 4.
        uint16_t fieldOffset = 0;
        std::memcpy(&fieldOffset, buffer + vtablePos + 4, sizeof(uint16_t));

        // TransactionData struct is 40 bytes (8 + 8 + 8 + 8 + 1 + 7 pad).
        // Crucially, validate against the table's declared objectSize, not just
        // the whole FlatBuffer. Otherwise a forged field offset could point into
        // unrelated bytes that happen to remain inside the payload.
        constexpr size_t STRUCT_SIZE = 40;
        if (fieldOffset < sizeof(int32_t)
                || fieldOffset > objectSize
                || STRUCT_SIZE > static_cast<size_t>(objectSize - fieldOffset)) {
            return false;
        }

        const size_t structPos = static_cast<size_t>(rootOffset) + fieldOffset;

        // 6. Direct memory reads for struct fields
        std::memcpy(&out.transactionId, buffer + structPos + 0, sizeof(uint64_t));
        std::memcpy(&out.amount,        buffer + structPos + 8, sizeof(int64_t));
        std::memcpy(&out.fromAccount,   buffer + structPos + 16, sizeof(uint64_t));
        std::memcpy(&out.toAccount,     buffer + structPos + 24, sizeof(uint64_t));
        out.type = buffer[structPos + 32];

        return true;
    }
};

} // namespace g12
