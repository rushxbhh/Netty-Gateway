#pragma once

#include <cstdint>
#include <atomic>
#include <cstring>
#include <functional>

#include "ring_layout.hpp"
#include "ring_buffer.hpp"
#include "flatbuffer_view.hpp"
#include "hardware.hpp"

namespace g12 {

/**
 * High-performance sequencer statistics.
 */
struct SequencerStats {
    uint64_t processed_count = 0;
    uint64_t malformed_count = 0;
    uint64_t empty_spin_cycles = 0;
    uint64_t journal_full_wait_cycles = 0;
};

/**
 * Ultra-Low-Latency Financial Sequencer Core Engine.
 * Pinned to an isolated CPU core, consumes inbound messages, assigns strict monotonic
 * sequence numbers, generates RDTSC timestamps, and writes 64B cache-aligned journal records.
 */
class SequencerEngine {
public:
    SequencerEngine(RingBuffer& inbound_ring, RingBuffer& journal_ring)
        : inbound_(inbound_ring), journal_(journal_ring) {}

    /**
     * Executes crash-recovery check on startup as specified in Section 7 of ring-layout.md:
     * If the sequencer crashed between publishing a journal slot and advancing inbound consumerSeq,
     * it detects whether the current inbound slot was already recorded in the last journal slot.
     */
    void recover_and_sync() noexcept {
        uint64_t j_prod = journal_.producer_seq().load(std::memory_order_acquire);
        uint64_t in_cons = inbound_.consumer_seq().load(std::memory_order_relaxed);
        uint64_t in_prod = inbound_.producer_seq().load(std::memory_order_acquire);

        if (j_prod > 0 && in_cons < in_prod) {
            // Read last journal slot
            const auto* last_journal = reinterpret_cast<const JournalSlot*>(
                journal_.slot_ptr(j_prod - 1)
            );
            // Read current inbound slot
            const auto* current_inbound = reinterpret_cast<const InboundSlot*>(
                inbound_.slot_ptr(in_cons)
            );

            // If connectionId matches, check if transaction already published
            if (current_inbound->connectionId == last_journal->connectionId) {
                ParsedTransaction txn{};
                bool parsed = FlatBufferView::try_parse(
                    current_inbound->payload,
                    current_inbound->payloadLen,
                    txn
                );
                if ((parsed && txn.transactionId == last_journal->clientTxnId) ||
                    (!parsed && last_journal->status == static_cast<uint8_t>(JournalStatus::REJECTED_MALFORMED))) {
                    // Already processed into journal prior to crash; advance consumerSeq
                    inbound_.consumer_seq().store(in_cons + 1, std::memory_order_release);
                }
            }
        }
    }

    /**
     * Processes a single incoming message from the inbound ring if available.
     * Returns true if a message was sequenced; false if inbound ring was empty.
     */
    bool step() noexcept {
        // 1. Read counters: own consumer counter is relaxed, producer counter is acquire
        const uint64_t in_c = inbound_.consumer_seq().load(std::memory_order_relaxed);
        const uint64_t in_p = inbound_.producer_seq().load(std::memory_order_acquire);

        // Inbound ring is empty
        if (in_c == in_p) {
            stats_.empty_spin_cycles++;
            return false;
        }

        // 2. Journal ring check: must wait if journal ring is full
        const uint64_t j_capacity = journal_.capacity();
        uint64_t j_p = journal_.producer_seq().load(std::memory_order_relaxed);
        uint64_t j_c = journal_.consumer_seq().load(std::memory_order_acquire);

        while (j_p - j_c >= j_capacity) {
            stats_.journal_full_wait_cycles++;
            hw::spin_pause();
            j_c = journal_.consumer_seq().load(std::memory_order_acquire);
        }

        // 3. Read inbound slot
        const auto* inbound_slot = reinterpret_cast<const InboundSlot*>(
            inbound_.slot_ptr(in_c)
        );

        // 4. Construct Journal slot (64 bytes, cache-aligned)
        JournalSlot journal_slot{};
        journal_slot.sequenceId = j_p;
        journal_slot.timestampTsc = hw::rdtsc();
        journal_slot.connectionId = inbound_slot->connectionId;

        // 5. Unpack FlatBuffer transaction
        ParsedTransaction txn{};
        bool valid = (inbound_slot->payloadLen <= INBOUND_PAYLOAD_MAX) &&
                     FlatBufferView::try_parse(inbound_slot->payload, inbound_slot->payloadLen, txn);

        if (valid) {
            journal_slot.clientTxnId = txn.transactionId;
            journal_slot.fromAccount  = txn.fromAccount;
            journal_slot.toAccount    = txn.toAccount;
            journal_slot.amount       = txn.amount;
            journal_slot.type         = txn.type;
            journal_slot.status       = static_cast<uint8_t>(JournalStatus::SEQUENCED_OK);
        } else {
            // Malformed payload: keep connectionId, zero other business fields
            journal_slot.clientTxnId = 0;
            journal_slot.fromAccount  = 0;
            journal_slot.toAccount    = 0;
            journal_slot.amount       = 0;
            journal_slot.type         = 0;
            journal_slot.status       = static_cast<uint8_t>(JournalStatus::REJECTED_MALFORMED);
            stats_.malformed_count++;
        }
        journal_slot.reserved = 0;

        // 6. Compute hardware CRC32C over the first 60 bytes (offset 0..59)
        constexpr size_t CHECKSUM_PAYLOAD_LEN = 60;
        journal_slot.checksum = hw::crc32c(&journal_slot, CHECKSUM_PAYLOAD_LEN);

        // 7. Write to Journal slot
        auto* target_slot = reinterpret_cast<JournalSlot*>(journal_.slot_ptr(j_p));
        std::memcpy(target_slot, &journal_slot, sizeof(JournalSlot));

        // 8. Publish Journal slot (RELEASE store)
        // Downstream Java ledger can now safely read this slot
        journal_.producer_seq().store(j_p + 1, std::memory_order_release);

        // 9. Advance Inbound consumerSeq (RELEASE store)
        // CRITICAL ORDERING RULE: Advance inbound consumerSeq ONLY AFTER journal publish!
        inbound_.consumer_seq().store(in_c + 1, std::memory_order_release);

        stats_.processed_count++;
        return true;
    }

    /**
     * Hot-path loop pinned to dedicated CPU core.
     * Continuously sequences incoming transactions until stop_flag becomes true.
     */
    void run(const std::atomic<bool>& stop_flag) noexcept {
        while (!stop_flag.load(std::memory_order_relaxed)) {
            if (!step()) {
                hw::spin_pause();
            }
        }
    }

    [[nodiscard]] const SequencerStats& stats() const noexcept { return stats_; }
    void reset_stats() noexcept { stats_ = SequencerStats{}; }

private:
    RingBuffer& inbound_;
    RingBuffer& journal_;
    SequencerStats stats_{};
};

} // namespace g12
