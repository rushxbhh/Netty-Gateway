#include <iostream>
#include <string>
#include <atomic>
#include <csignal>
#include <chrono>
#include <thread>
#include <iomanip>

#include "sequencer/hardware.hpp"
#include "sequencer/ring_layout.hpp"
#include "sequencer/ring_buffer.hpp"
#include "sequencer/sequencer.hpp"

namespace {
std::atomic<bool> g_running{true};

void signal_handler(int) {
    g_running.store(false, std::memory_order_relaxed);
}
} // namespace

int main(int argc, char* argv[]) {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

#if defined(_WIN32) || defined(_WIN64)
    std::string inbound_path = "./g12-inbound.ring";
    std::string journal_path = "./g12-journal.ring";
#else
    std::string inbound_path = "/dev/shm/g12-inbound.ring";
    std::string journal_path = "/dev/shm/g12-journal.ring";
#endif

    int core_id = 2; // Default isolated core
    uint64_t idle_sleep_us = 0; // 0 keeps production spin-polling behavior
    bool pin_core = true;
    bool legacy_create_rings_flag = false;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--inbound" && i + 1 < argc) {
            inbound_path = argv[++i];
        } else if (arg == "--journal" && i + 1 < argc) {
            journal_path = argv[++i];
        } else if (arg == "--core" && i + 1 < argc) {
            core_id = std::stoi(argv[++i]);
        } else if (arg == "--idle-sleep-us" && i + 1 < argc) {
            idle_sleep_us = std::stoull(argv[++i]);
        } else if (arg == "--no-pin") {
            pin_core = false;
        } else if (arg == "--create-rings") {
            // Backwards-compatible no-op. Rings are initialized only when
            // their files are empty; an existing ring is never reset.
            legacy_create_rings_flag = true;
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: g12_sequencer [options]\n"
                      << "  --inbound <path>   Inbound ring path (default: " << inbound_path << ")\n"
                      << "  --journal <path>   Journal ring path (default: " << journal_path << ")\n"
                      << "  --core <id>        Core ID to pin thread to (default: " << core_id << ")\n"
                      << "  --idle-sleep-us <n> Sleep when idle (development mode; default 0 = spin)\n"
                      << "  --no-pin           Disable core pinning\n"
                      << "  --create-rings     Deprecated no-op; empty rings initialize automatically\n"
                      << "  --help, -h         Show this message\n";
            return 0;
        }
    }

    std::cout << "========================================================\n"
              << "  G12 Ultra-Low-Latency Financial Sequencer Engine (C++)\n"
              << "========================================================\n";
    if (legacy_create_rings_flag) {
        std::cout << "[Startup] --create-rings is deprecated; existing rings will not be reset.\n";
    }

    // 1. Core Pinning
    if (pin_core) {
        if (g12::hw::set_thread_affinity(core_id)) {
            std::cout << "[Affinity] Pinned sequencer thread to isolated CPU core: " << core_id << "\n";
        } else {
            std::cerr << "[Affinity] Warning: Failed to pin thread to core " << core_id << " (running unpinned)\n";
        }
    } else {
        std::cout << "[Affinity] Running unpinned (--no-pin specified)\n";
    }

    // 2. Open / Map Inbound Ring
    // The ring is initialized only if its file is empty. Existing rings are
    // attached without resetting their counters or slots. The startup file
    // lock is compatible with Java FileChannel.lock(), preventing the Gateway
    // and Sequencer from racing while creating/initializing the same file.
    const size_t inbound_size = g12::ring_total_file_size(g12::INBOUND_CAPACITY, g12::INBOUND_SLOT_SIZE);
    g12::MmapFile inbound_mmap;
    bool inbound_was_empty = false;
    if (!inbound_mmap.open_or_create(inbound_path, inbound_size, true, &inbound_was_empty)) {
        std::cerr << "[Inbound] Error: Unable to open or map inbound file (missing, truncated, or inaccessible): "
                  << inbound_path << "\n";
        return 1;
    }
    if (inbound_was_empty) {
        if (!g12::RingBuffer::initialize_ring(
                inbound_mmap.data(),
                g12::RING_KIND_INBOUND,
                g12::INBOUND_CAPACITY,
                g12::INBOUND_SLOT_SIZE)) {
            std::cerr << "[Inbound] Error: Failed to initialize new inbound ring\n";
            return 1;
        }
        std::cout << "[Inbound] Initialized new ring file.\n";
    }

    g12::RingBuffer inbound_ring;
    if (!inbound_ring.attach(inbound_mmap.data(), g12::RING_KIND_INBOUND, g12::INBOUND_CAPACITY, g12::INBOUND_SLOT_SIZE)) {
        std::cerr << "[Inbound] Error: Inbound ring header/layout mismatch\n";
        return 1;
    }
    inbound_mmap.unlock_startup();
    std::cout << "[Inbound] Attached to " << inbound_path << " (" << (inbound_size / (1024 * 1024)) << " MiB, "
              << g12::INBOUND_CAPACITY << " slots x " << g12::INBOUND_SLOT_SIZE << " B; payload max "
              << g12::INBOUND_PAYLOAD_MAX << " B)\n";

    // 3. Open / Map Journal Ring independently.
    // A new journal file gets initialized here, while an existing journal is
    // preserved. This never reinitializes an existing inbound ring.
    const size_t journal_size = g12::ring_total_file_size(g12::JOURNAL_CAPACITY, g12::JOURNAL_SLOT_SIZE);
    g12::MmapFile journal_mmap;
    bool journal_was_empty = false;
    if (!journal_mmap.open_or_create(journal_path, journal_size, true, &journal_was_empty)) {
        std::cerr << "[Journal] Error: Unable to open or map journal file (missing, truncated, or inaccessible): "
                  << journal_path << "\n";
        return 1;
    }
    if (journal_was_empty) {
        if (!g12::RingBuffer::initialize_ring(
                journal_mmap.data(),
                g12::RING_KIND_JOURNAL,
                g12::JOURNAL_CAPACITY,
                g12::JOURNAL_SLOT_SIZE)) {
            std::cerr << "[Journal] Error: Failed to initialize new journal ring\n";
            return 1;
        }
        std::cout << "[Journal] Initialized new ring file.\n";
    }

    g12::RingBuffer journal_ring;
    if (!journal_ring.attach(journal_mmap.data(), g12::RING_KIND_JOURNAL, g12::JOURNAL_CAPACITY, g12::JOURNAL_SLOT_SIZE)) {
        std::cerr << "[Journal] Error: Journal ring header/layout mismatch\n";
        return 1;
    }
    journal_mmap.unlock_startup();
    std::cout << "[Journal] Attached to " << journal_path << " (" << (journal_size / (1024 * 1024)) << " MiB, "
              << g12::JOURNAL_CAPACITY << " slots x " << g12::JOURNAL_SLOT_SIZE << " B)\n";

    // 4. Initialize Sequencer & Perform Recovery Check
    g12::SequencerEngine sequencer(inbound_ring, journal_ring);
    sequencer.recover_and_sync();

    uint64_t start_in_c = inbound_ring.consumer_seq().load(std::memory_order_relaxed);
    uint64_t start_j_p = journal_ring.producer_seq().load(std::memory_order_relaxed);
    std::cout << "[Sequencer] Recovery sync complete. Initial Inbound consumerSeq=" << start_in_c
              << ", Journal producerSeq=" << start_j_p << "\n";
    std::cout << "[Sequencer] Hot-path active. Polling lock-free rings...\n";
    if (idle_sleep_us > 0) {
        std::cout << "[Sequencer] Development idle sleep: " << idle_sleep_us << " us\n";
    }

    // 5. Run Sequencer loop
    auto start_time = std::chrono::steady_clock::now();
    sequencer.run(g_running, std::chrono::microseconds(idle_sleep_us));
    auto end_time = std::chrono::steady_clock::now();

    // 6. Report Stats
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    const auto& stats = sequencer.stats();

    std::cout << "\n================ Sequencer Stats ================\n"
              << "  Uptime:                " << elapsed_ms << " ms\n"
              << "  Sequenced Records:     " << stats.processed_count << "\n"
              << "  Malformed Records:     " << stats.malformed_count << "\n"
              << "  Empty Spin Cycles:     " << stats.empty_spin_cycles << "\n"
              << "  Journal Full Stalls:   " << stats.journal_full_wait_cycles << "\n"
              << "  Final Journal Seq:     " << journal_ring.producer_seq().load(std::memory_order_relaxed) << "\n"
              << "=================================================\n";

    return 0;
}
