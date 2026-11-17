#include <iostream>
#ifdef NDEBUG
#undef NDEBUG
#endif
#include <cassert>
#include <vector>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
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

    // Test corrupted identifier.
    auto bad_identifier = valid_fb;
    bad_identifier[4] = 'X';
    ok = g12::FlatBufferView::try_parse(bad_identifier.data(), bad_identifier.size(), out);
    assert(!ok);

    // The struct still fits inside the whole payload, but the vtable field
    // offset places it beyond the table's declared objectSize. This must fail.
    auto field_outside_table = build_test_flatbuffer(9999ULL, 12500LL, 1001ULL, 2002ULL, 0);
    const uint16_t forged_field_offset = 8; // objectSize is 44; 8 + 40 exceeds it
    std::memcpy(field_outside_table.data() + 12, &forged_field_offset, sizeof(forged_field_offset));
    ok = g12::FlatBufferView::try_parse(field_outside_table.data(), field_outside_table.size(), out);
    assert(!ok);

    std::cout << "  Passed (valid payload parsed; bad identifier and out-of-object field rejected)\n";
}

void test_ring_layout_and_sizes() {
    std::cout << "[Test 3] Testing Ring Layout and Memory Alignments...\n";
    assert(sizeof(g12::RingHeaderLine) == 64);
    assert(sizeof(g12::InboundSlot) == 128);
    assert(offsetof(g12::InboundSlot, clientTxnId) == 16);
    assert(offsetof(g12::InboundSlot, payload) == 32);
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


void test_malformed_parser_keeps_gateway_transaction_id() {
    std::cout << "[Test 5] Testing Transaction ID Fallback for Parser Rejection...\n";

    constexpr uint64_t CAPACITY = 8;
    std::vector<uint8_t> inbound_memory(
        g12::ring_total_file_size(CAPACITY, g12::INBOUND_SLOT_SIZE), 0);
    std::vector<uint8_t> journal_memory(
        g12::ring_total_file_size(CAPACITY, g12::JOURNAL_SLOT_SIZE), 0);

    assert(g12::RingBuffer::initialize_ring(
        inbound_memory.data(), g12::RING_KIND_INBOUND, CAPACITY, g12::INBOUND_SLOT_SIZE));
    assert(g12::RingBuffer::initialize_ring(
        journal_memory.data(), g12::RING_KIND_JOURNAL, CAPACITY, g12::JOURNAL_SLOT_SIZE));

    g12::RingBuffer inbound_ring;
    g12::RingBuffer journal_ring;
    assert(inbound_ring.attach(
        inbound_memory.data(), g12::RING_KIND_INBOUND, CAPACITY, g12::INBOUND_SLOT_SIZE));
    assert(journal_ring.attach(
        journal_memory.data(), g12::RING_KIND_JOURNAL, CAPACITY, g12::JOURNAL_SLOT_SIZE));

    constexpr uint64_t CLIENT_TXN_ID = 987654321;
    auto malformed = build_test_flatbuffer(CLIENT_TXN_ID, 500, 10, 20, 0);
    malformed[4] = 'B';
    malformed[5] = 'A';
    malformed[6] = 'D';
    malformed[7] = '!';

    auto* inbound_slot = reinterpret_cast<g12::InboundSlot*>(inbound_ring.slot_ptr(0));
    inbound_slot->payloadLen = static_cast<uint32_t>(malformed.size());
    inbound_slot->flags = 0;
    inbound_slot->connectionId = 42;
    inbound_slot->clientTxnId = CLIENT_TXN_ID;
    std::memcpy(inbound_slot->payload, malformed.data(), malformed.size());
    inbound_ring.producer_seq().store(1, std::memory_order_release);

    g12::SequencerEngine sequencer(inbound_ring, journal_ring);
    assert(sequencer.step());

    const auto* journal_slot = reinterpret_cast<const g12::JournalSlot*>(journal_ring.slot_ptr(0));
    assert(journal_slot->sequenceId == 0);
    assert(journal_slot->connectionId == 42);
    assert(journal_slot->clientTxnId == CLIENT_TXN_ID);
    assert(journal_slot->status == static_cast<uint8_t>(g12::JournalStatus::REJECTED_MALFORMED));
    assert(journal_slot->checksum == g12::hw::crc32c(journal_slot, 60));
    assert(inbound_ring.consumer_seq().load(std::memory_order_acquire) == 1);

    std::cout << "  Passed (malformed journal record retains transaction ID for ACK correlation)\n";
}

void test_mmap_ring_restart_is_non_destructive() {
    std::cout << "[Test 6] Testing Ring Initialization and Restart Preservation...\n";

    constexpr uint64_t CAPACITY = 16;
    const size_t required_size = g12::ring_total_file_size(CAPACITY, g12::INBOUND_SLOT_SIZE);
    const auto unique_suffix = std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count()
    );
    const std::filesystem::path ring_path =
        std::filesystem::temp_directory_path() / ("g12-inbound-restart-" + unique_suffix + ".ring");

    // First process: create and initialize an empty ring, then simulate
    // non-zero counters left behind by a running producer/consumer pair.
    {
        g12::MmapFile mapped_file;
        bool was_empty = false;
        assert(mapped_file.open_or_create(ring_path.string(), required_size, true, &was_empty));
        assert(was_empty);
        assert(g12::RingBuffer::initialize_ring(
            mapped_file.data(),
            g12::RING_KIND_INBOUND,
            CAPACITY,
            g12::INBOUND_SLOT_SIZE
        ));

        g12::RingBuffer ring;
        assert(ring.attach(mapped_file.data(), g12::RING_KIND_INBOUND, CAPACITY, g12::INBOUND_SLOT_SIZE));
        ring.producer_seq().store(7, std::memory_order_relaxed);
        ring.consumer_seq().store(5, std::memory_order_relaxed);
        mapped_file.unlock_startup();
    }

    // Restart: the existing ring must be attached without resetting counters.
    {
        g12::MmapFile mapped_file;
        bool was_empty = true;
        assert(mapped_file.open_or_create(ring_path.string(), required_size, true, &was_empty));
        assert(!was_empty);

        g12::RingBuffer ring;
        assert(ring.attach(mapped_file.data(), g12::RING_KIND_INBOUND, CAPACITY, g12::INBOUND_SLOT_SIZE));
        assert(ring.producer_seq().load(std::memory_order_relaxed) == 7);
        assert(ring.consumer_seq().load(std::memory_order_relaxed) == 5);
        mapped_file.unlock_startup();
    }

    std::filesystem::remove(ring_path);

    // A non-empty but undersized file is corruption, not a new ring. It must
    // fail rather than being expanded and silently initialized.
    const auto truncated_path = ring_path.string() + ".truncated";
    {
        std::ofstream truncated_file(truncated_path, std::ios::binary);
        truncated_file.put('X');
    }
    {
        g12::MmapFile mapped_file;
        bool was_empty = false;
        assert(!mapped_file.open_or_create(truncated_path, required_size, true, &was_empty));
    }
    std::filesystem::remove(truncated_path);

    std::cout << "  Passed (existing counters preserved; truncated ring rejected)\n";
}

void test_shutdown_while_journal_full() {
    std::cout << "[Test 7] Testing Shutdown While Journal Ring Is Full...\n";

    constexpr uint64_t CAPACITY = 2;
    std::vector<uint8_t> inbound_memory(
        g12::ring_total_file_size(CAPACITY, g12::INBOUND_SLOT_SIZE), 0);
    std::vector<uint8_t> journal_memory(
        g12::ring_total_file_size(CAPACITY, g12::JOURNAL_SLOT_SIZE), 0);

    assert(g12::RingBuffer::initialize_ring(
        inbound_memory.data(), g12::RING_KIND_INBOUND, CAPACITY, g12::INBOUND_SLOT_SIZE));
    assert(g12::RingBuffer::initialize_ring(
        journal_memory.data(), g12::RING_KIND_JOURNAL, CAPACITY, g12::JOURNAL_SLOT_SIZE));

    g12::RingBuffer inbound_ring;
    g12::RingBuffer journal_ring;
    assert(inbound_ring.attach(
        inbound_memory.data(), g12::RING_KIND_INBOUND, CAPACITY, g12::INBOUND_SLOT_SIZE));
    assert(journal_ring.attach(
        journal_memory.data(), g12::RING_KIND_JOURNAL, CAPACITY, g12::JOURNAL_SLOT_SIZE));

    // One inbound item is waiting, but the journal is already full. The run
    // loop must observe SIGTERM-equivalent cancellation while waiting for Audit.
    inbound_ring.producer_seq().store(1, std::memory_order_release);
    journal_ring.producer_seq().store(CAPACITY, std::memory_order_release);
    journal_ring.consumer_seq().store(0, std::memory_order_release);

    g12::SequencerEngine sequencer(inbound_ring, journal_ring);
    std::atomic<bool> running{true};
    std::thread stop_request([&running]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        running.store(false, std::memory_order_relaxed);
    });

    sequencer.run(running);
    stop_request.join();

    assert(!running.load(std::memory_order_relaxed));
    assert(inbound_ring.consumer_seq().load(std::memory_order_relaxed) == 0);
    assert(journal_ring.producer_seq().load(std::memory_order_relaxed) == CAPACITY);
    std::cout << "  Passed (stop request observed without releasing the blocked journal slot)\n";
}

void test_latency_benchmark() {
    std::cout << "[Test 8] Running Hot-Path Throughput & Latency Benchmark...\n";

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
    test_malformed_parser_keeps_gateway_transaction_id();
    test_mmap_ring_restart_is_non_destructive();
    test_shutdown_while_journal_full();
    test_latency_benchmark();

    std::cout << "====================================================\n"
              << "   ALL VERIFICATION & BENCHMARK TESTS PASSED!\n"
              << "====================================================\n";
    return 0;
}
