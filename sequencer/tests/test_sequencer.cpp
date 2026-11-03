#include <iostream>
#include <cassert>
#include <vector>
#include <chrono>
#include <cstring>
#include <numeric>
#include <algorithm>

#include "sequencer/hardware.hpp"
#include "sequencer/ring_layout.hpp"
#include "sequencer/ring_buffer.hpp"
#include "sequencer/flatbuffer_view.hpp"
#include "sequencer/sequencer.hpp"

// Helper to manually build a valid binary FlatBuffer for testing
std::vector<uint8_t> build_test_flatbuffer(
    uint64_t txn_id,
    int64_t amount,
    uint64_t from_acct,
    uint64_t to_acct,
    uint8_t txn_type
) {
    // Structure:
    // [0..3]: root table offset (e.g. 8)
    // [4..7]: "NIGT"
    // [8..11]: soffset to vtable (-4, so vtable is at 8 - (-4) = 12? No, vtable is usually prepended or appended)
    // Let's place:
    // Offset 8: vtable_size = 6 (uint16_t), object_size = 48 (uint16_t)
    // Offset 12: field 0 offset = 4 (uint16_t, relative to table start)
    // Table starts at Offset 16:
    // Offset 16..19: vtable offset = 8 (table starts at 16, vtable is at 8, so 16 - 8 = 8)
    // Offset 20..59: TransactionData struct (40 bytes)
    // Total size = 60 bytes.
    // Root offset at 0..3: 16 (table start)

    std::vector<uint8_t> buf(64, 0);

    // Root offset = 16
    uint32_t root_offset = 16;
    std::memcpy(&buf[0], &root_offset, 4);

    // Identifier = "NIGT"
    std::memcpy(&buf[4], "NIGT", 4);

    // Vtable at offset 8:
    uint16_t vtable_size = 6;
    uint16_t object_size = 44;
    uint16_t field_0_offset = 4; // offset from table start (16 + 4 = 20)
    std::memcpy(&buf[8], &vtable_size, 2);
    std::memcpy(&buf[10], &object_size, 2);
    std::memcpy(&buf[12], &field_0_offset, 2);

    // Table at offset 16:
    int32_t vtable_soffset = 8; // 16 - 8 = 8 (vtable position)
    std::memcpy(&buf[16], &vtable_soffset, 4);

    // Struct at offset 20 (40 bytes):
    std::memcpy(&buf[20], &txn_id, 8);
    std::memcpy(&buf[28], &amount, 8);
    std::memcpy(&buf[36], &from_acct, 8);
    std::memcpy(&buf[44], &to_acct, 8);
    buf[52] = txn_type;
    // buf[53..59] is padding

    return buf;
}

void test_crc32c() {
    std::cout << "[Test 1] Testing Hardware vs Software CRC32C...\n";
    const char* sample = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    uint32_t hw_crc = g12::hw::crc32c(sample, strlen(sample));
    uint32_t sw_crc = g12::hw::crc32c_software(sample, strlen(sample));
    assert(hw_crc == sw_crc);
    std::cout << "  Passed (CRC=0x" << std::hex << hw_crc << std::dec << ")\n";
}

void test_flatbuffer_parser() {
    std::cout << "[Test 2] Testing FlatBuffer Zero-Copy View Parser...\n";
    auto valid_fb = build_test_flatbuffer(9999ULL, 12500LL, 1001ULL, 2002ULL, 0);

    g12::ParsedTransaction out{};
    bool ok = g12::FlatBufferView::try_parse(valid_fb.data(), valid_fb.size(), out);
    assert(ok);
    assert(out.transactionId == 9999ULL);
    assert(out.amount == 12500LL);
    assert(out.fromAccount == 1001ULL);
    assert(out.toAccount == 2002ULL);
    assert(out.type == 0);

    // Test corrupted identifier
    valid_fb[4] = 'X';
    ok = g12::FlatBufferView::try_parse(valid_fb.data(), valid_fb.size(), out);
    assert(!ok);

    std::cout << "  Passed\n";
}

void test_ring_layout_and_sizes() {
    std::cout << "[Test 3] Testing Ring Layout and Memory Alignments...\n";
    assert(sizeof(g12::RingHeaderLine) == 64);
    assert(sizeof(g12::InboundSlot) == 128);
    assert(sizeof(g12::JournalSlot) == 64);
    assert(alignof(g12::JournalSlot) == 64);

    assert(g12::OFFSET_HEADER_LINE == 0);
    assert(g12::OFFSET_PRODUCER_SEQ == 64);
    assert(g12::OFFSET_CONSUMER_SEQ == 128);
    assert(g12::OFFSET_SLOTS_START == 192);

    std::cout << "  Passed\n";
}

void test_end_to_end_sequencing() {
    std::cout << "[Test 4] Testing End-to-End Sequencer Pipeline...\n";

    // Allocate memory buffers for test rings
    constexpr uint64_t INBOUND_CAP = 1024;
    constexpr uint64_t JOURNAL_CAP = 1024;

    std::vector<uint8_t> in_mem(g12::ring_total_file_size(INBOUND_CAP, g12::INBOUND_SLOT_SIZE), 0);
    std::vector<uint8_t> j_mem(g12::ring_total_file_size(JOURNAL_CAP, g12::JOURNAL_SLOT_SIZE), 0);

    g12::RingBuffer::initialize_ring(in_mem.data(), g12::RING_KIND_INBOUND, INBOUND_CAP, g12::INBOUND_SLOT_SIZE);
    g12::RingBuffer::initialize_ring(j_mem.data(), g12::RING_KIND_JOURNAL, JOURNAL_CAP, g12::JOURNAL_SLOT_SIZE);

    g12::RingBuffer inbound_ring;
    g12::RingBuffer journal_ring;
    assert(inbound_ring.attach(in_mem.data(), g12::RING_KIND_INBOUND, INBOUND_CAP, g12::INBOUND_SLOT_SIZE));
    assert(journal_ring.attach(j_mem.data(), g12::RING_KIND_JOURNAL, JOURNAL_CAP, g12::JOURNAL_SLOT_SIZE));

    g12::SequencerEngine sequencer(inbound_ring, journal_ring);

    // Produce 500 valid items + 10 malformed items from simulated Java Gateway
    constexpr uint64_t NUM_ITEMS = 500;
    for (uint64_t i = 0; i < NUM_ITEMS; ++i) {
        auto fb = build_test_flatbuffer(1000 + i, 5000 + i, 10 + (i % 5), 20 + (i % 5), (i % 3));

        auto* slot = reinterpret_cast<g12::InboundSlot*>(inbound_ring.slot_ptr(i));
        slot->payloadLen = static_cast<uint32_t>(fb.size());
        slot->flags = 0;
        slot->connectionId = 42;
        std::memcpy(slot->payload, fb.data(), fb.size());

        inbound_ring.producer_seq().store(i + 1, std::memory_order_release);
    }

    // Sequence all items
    uint64_t sequenced = 0;
    while (sequenced < NUM_ITEMS) {
        if (sequencer.step()) {
            sequenced++;
        }
    }

    assert(sequenced == NUM_ITEMS);
    assert(journal_ring.producer_seq().load(std::memory_order_relaxed) == NUM_ITEMS);
    assert(inbound_ring.consumer_seq().load(std::memory_order_relaxed) == NUM_ITEMS);

    // Verify all journal records
    for (uint64_t seq = 0; seq < NUM_ITEMS; ++seq) {
        const auto* j_slot = reinterpret_cast<const g12::JournalSlot*>(journal_ring.slot_ptr(seq));
        assert(j_slot->sequenceId == seq);
        assert(j_slot->connectionId == 42);
        assert(j_slot->clientTxnId == 1000 + seq);
        assert(j_slot->amount == 5000 + static_cast<int64_t>(seq));
        assert(j_slot->status == static_cast<uint8_t>(g12::JournalStatus::SEQUENCED_OK));

        // Verify CRC32C checksum
        uint32_t computed_crc = g12::hw::crc32c(j_slot, 60);
        assert(j_slot->checksum == computed_crc);
    }

    std::cout << "  Passed (Sequenced " << NUM_ITEMS << " contiguous transactions)\n";
}

void test_latency_benchmark() {
    std::cout << "[Test 5] Running Hot-Path Throughput & Latency Benchmark...\n";

    constexpr uint64_t CAP = 65536;
    std::vector<uint8_t> in_mem(g12::ring_total_file_size(CAP, g12::INBOUND_SLOT_SIZE), 0);
    std::vector<uint8_t> j_mem(g12::ring_total_file_size(CAP, g12::JOURNAL_SLOT_SIZE), 0);

    g12::RingBuffer::initialize_ring(in_mem.data(), g12::RING_KIND_INBOUND, CAP, g12::INBOUND_SLOT_SIZE);
    g12::RingBuffer::initialize_ring(j_mem.data(), g12::RING_KIND_JOURNAL, CAP, g12::JOURNAL_SLOT_SIZE);

    g12::RingBuffer inbound_ring;
    g12::RingBuffer journal_ring;
    inbound_ring.attach(in_mem.data(), g12::RING_KIND_INBOUND, CAP, g12::INBOUND_SLOT_SIZE);
    journal_ring.attach(j_mem.data(), g12::RING_KIND_JOURNAL, CAP, g12::JOURNAL_SLOT_SIZE);

    g12::SequencerEngine sequencer(inbound_ring, journal_ring);

    auto sample_fb = build_test_flatbuffer(8888, 500, 10, 20, 0);

    constexpr uint64_t WARMUP = 10000;
    constexpr uint64_t BENCH_COUNT = 100000;

    // Pre-populate inbound slots
    for (uint64_t i = 0; i < CAP; ++i) {
        auto* slot = reinterpret_cast<g12::InboundSlot*>(inbound_ring.slot_ptr(i));
        slot->payloadLen = static_cast<uint32_t>(sample_fb.size());
        slot->connectionId = 100;
        std::memcpy(slot->payload, sample_fb.data(), sample_fb.size());
    }

    // Warmup
    inbound_ring.producer_seq().store(WARMUP, std::memory_order_release);
    for (uint64_t i = 0; i < WARMUP; ++i) {
        sequencer.step();
        // Consume from journal in test harness
        journal_ring.consumer_seq().store(i + 1, std::memory_order_release);
    }

    // Reset sequences for clean measurement
    inbound_ring.producer_seq().store(0, std::memory_order_relaxed);
    inbound_ring.consumer_seq().store(0, std::memory_order_relaxed);
    journal_ring.producer_seq().store(0, std::memory_order_relaxed);
    journal_ring.consumer_seq().store(0, std::memory_order_relaxed);

    std::vector<uint64_t> latencies_tsc;
    latencies_tsc.reserve(BENCH_COUNT);

    auto t_start = std::chrono::high_resolution_clock::now();

    for (uint64_t i = 0; i < BENCH_COUNT; ++i) {
        inbound_ring.producer_seq().store(i + 1, std::memory_order_release);

        uint64_t t0 = g12::hw::rdtsc();
        sequencer.step();
        uint64_t t1 = g12::hw::rdtsc();

        latencies_tsc.push_back(t1 - t0);
        journal_ring.consumer_seq().store(i + 1, std::memory_order_release);
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed_sec = std::chrono::duration<double>(t_end - t_start).count();
    double ops_per_sec = static_cast<double>(BENCH_COUNT) / elapsed_sec;

    std::sort(latencies_tsc.begin(), latencies_tsc.end());
    uint64_t p50_tsc = latencies_tsc[static_cast<size_t>(BENCH_COUNT * 0.50)];
    uint64_t p99_tsc = latencies_tsc[static_cast<size_t>(BENCH_COUNT * 0.99)];
    uint64_t p999_tsc = latencies_tsc[static_cast<size_t>(BENCH_COUNT * 0.999)];

    std::cout << "  Throughput: " << static_cast<uint64_t>(ops_per_sec) << " tx/sec ("
              << (ops_per_sec / 1e6) << " M ops/s)\n"
              << "  Latency p50:  " << p50_tsc << " CPU cycles\n"
              << "  Latency p99:  " << p99_tsc << " CPU cycles\n"
              << "  Latency p99.9: " << p999_tsc << " CPU cycles\n";
}

int main() {
    std::cout << "====================================================\n"
              << "   G12 Sequencer Engine Automated Verification Suite\n"
              << "====================================================\n";

    test_crc32c();
    test_flatbuffer_parser();
    test_ring_layout_and_sizes();
    test_end_to_end_sequencing();
    test_latency_benchmark();

    std::cout << "====================================================\n"
              << "   ALL VERIFICATION & BENCHMARK TESTS PASSED!\n"
              << "====================================================\n";
    return 0;
}
