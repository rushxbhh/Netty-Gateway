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
    bool pin_core = true;
    bool create_rings = false;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--inbound" && i + 1 < argc) {
            inbound_path = argv[++i];
        } else if (arg == "--journal" && i + 1 < argc) {
            journal_path = argv[++i];
        } else if (arg == "--core" && i + 1 < argc) {
            core_id = std::stoi(argv[++i]);
        } else if (arg == "--no-pin") {
            pin_core = false;
        } else if (arg == "--create-rings") {
            create_rings = true;
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: g12_sequencer [options]\n"
                      << "  --inbound <path>   Inbound ring path (default: " << inbound_path << ")\n"
                      << "  --journal <path>   Journal ring path (default: " << journal_path << ")\n"
                      << "  --core <id>        Core ID to pin thread to (default: " << core_id << ")\n"
                      << "  --no-pin           Disable core pinning\n"
                      << "  --create-rings     Initialize ring files if not already present\n"
                      << "  --help, -h         Show this message\n";
            return 0;
        }
    }

    std::cout << "========================================================\n"
              << "  G12 Ultra-Low-Latency Financial Sequencer Engine (C++)\n"
              << "========================================================\n";

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
    const size_t inbound_size = g12::ring_total_file_size(g12::INBOUND_CAPACITY, g12::INBOUND_SLOT_SIZE);
    g12::MmapFile inbound_mmap;
    if (!inbound_mmap.open_or_create(inbound_path, inbound_size, create_rings)) {
        std::cerr << "[Inbound] Error: Unable to open or map inbound file: " << inbound_path << "\n";
        return 1;
    }
    if (create_rings) {
        g12::RingBuffer::initialize_ring(
            inbound_mmap.data(),
            g12::RING_KIND_INBOUND,
            g12::INBOUND_CAPACITY,
            g12::INBOUND_SLOT_SIZE
        );
    }

    g12::RingBuffer inbound_ring;
    if (!inbound_ring.attach(inbound_mmap.data(), g12::RING_KIND_INBOUND, g12::INBOUND_CAPACITY, g12::INBOUND_SLOT_SIZE)) {
        std::cerr << "[Inbound] Error: Inbound ring magic or layout mismatch (waiting timed out)\n";
        return 1;
    }
    std::cout << "[Inbound] Attached to " << inbound_path << " (" << (inbound_size / (1024 * 1024)) << " MB, "
              << g12::INBOUND_CAPACITY << " slots @ 128B)\n";

    // 3. Open / Map Journal Ring
    const size_t journal_size = g12::ring_total_file_size(g12::JOURNAL_CAPACITY, g12::JOURNAL_SLOT_SIZE);
    g12::MmapFile journal_mmap;
    if (!journal_mmap.open_or_create(journal_path, journal_size, create_rings)) {
        std::cerr << "[Journal] Error: Unable to open or map journal file: " << journal_path << "\n";
        return 1;
    }
    if (create_rings) {
        g12::RingBuffer::initialize_ring(
            journal_mmap.data(),
            g12::RING_KIND_JOURNAL,
            g12::JOURNAL_CAPACITY,
            g12::JOURNAL_SLOT_SIZE
        );
    }

    g12::RingBuffer journal_ring;
    if (!journal_ring.attach(journal_mmap.data(), g12::RING_KIND_JOURNAL, g12::JOURNAL_CAPACITY, g12::JOURNAL_SLOT_SIZE)) {
        std::cerr << "[Journal] Error: Journal ring magic or layout mismatch (waiting timed out)\n";
        return 1;
    }
    std::cout << "[Journal] Attached to " << journal_path << " (" << (journal_size / (1024 * 1024)) << " MB, "
              << g12::JOURNAL_CAPACITY << " slots @ 64B)\n";

    // 4. Initialize Sequencer & Perform Recovery Check
    g12::SequencerEngine sequencer(inbound_ring, journal_ring);
    sequencer.recover_and_sync();

    uint64_t start_in_c = inbound_ring.consumer_seq().load(std::memory_order_relaxed);
    uint64_t start_j_p = journal_ring.producer_seq().load(std::memory_order_relaxed);
    std::cout << "[Sequencer] Recovery sync complete. Initial Inbound consumerSeq=" << start_in_c
              << ", Journal producerSeq=" << start_j_p << "\n";
    std::cout << "[Sequencer] Hot-path active. Polling lock-free rings...\n";

    // 5. Run Sequencer loop
    auto start_time = std::chrono::steady_clock::now();
    sequencer.run(g_running);
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
