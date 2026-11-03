# G12 Ultra-Low-Latency Sequencer Engine (C++20)

Ultra-low-latency deterministic sequencing engine for the **G12 Ultra-Low Latency Financial Ledger & Ingress Gateway** project.

Pinning itself to a dedicated CPU core, it connects the **Java Netty Gateway** and the **Java Audit Ledger** via lock-free Single-Producer Single-Consumer (SPSC) shared-memory ring buffers.

---

## Architecture & Data Flow

```
+-------------------------------------------------------+
|  Java Netty Ingress Gateway                           |
|  - Validates WebSocket client messages                |
|  - Copies FlatBuffer into /dev/shm/g12-inbound.ring   |
|  - Stores inbound.producerSeq (Release)               |
+---------------------------+---------------------------+
                            |
                            v [Lock-Free SPSC Inbound Ring: 128B slots, 64K capacity]
+-------------------------------------------------------+
|  C++ Sequencer Engine (Pinned CPU Core, Zero-Alloc)   |
|  - Spin polls inbound.producerSeq (Acquire)           |
|  - Zero-copy unpacks FlatBuffer payload               |
|  - Assigns monotonic sequenceId (0, 1, 2, ... no gaps)|
|  - Reads RDTSC hardware tick timestamp                |
|  - Computes hardware SSE4.2 CRC32C over first 60B     |
|  - Writes 64B cache-aligned JournalSlot (alignas 64)  |
|  - Publishes journal.producerSeq (Release)            |
|  - Advances inbound.consumerSeq (Release)             |
+---------------------------+---------------------------+
                            |
                            v [Lock-Free SPSC Journal Ring: 64B slots, 1M capacity]
+-------------------------------------------------------+
|  Java Ledger & Audit Engine                           |
|  - Polls journal.producerSeq (Acquire)                |
|  - Verifies CRC32C and sequence continuity            |
|  - Updates in-memory double-entry account balances    |
|  - Asynchronously replies to WebSocket clients        |
+-------------------------------------------------------+
```

---

## Directory Structure

```
ULL-FLIG/sequencer/
├── CMakeLists.txt              # Standard CMake build definition
├── Makefile                    # Direct Makefile for GCC / MinGW / Clang
├── README.md                   # Architecture, usage, and build instructions
├── include/
│   └── sequencer/
│       ├── hardware.hpp        # Core pinning, RDTSC timestamping, SSE4.2 CRC32C, spin pause
│       ├── ring_layout.hpp     # Cache-aligned structs (RingHeaderLine, InboundSlot, JournalSlot)
│       ├── ring_buffer.hpp     # Cross-platform MMAP SPSC Ring buffer (POSIX /dev/shm + Windows)
│       ├── flatbuffer_view.hpp # Zero-copy, zero-allocation "NIGT" FlatBuffer binary parser
│       └── sequencer.hpp       # Core sequencing loop and startup crash recovery sync
├── src/
│   └── main.cpp                # Sequencer daemon CLI entrypoint & signal handling
└── tests/
    └── test_sequencer.cpp      # Automated verification suite & throughput/latency benchmark
```

---

## Building and Running

### Prerequisites
- Modern C++ compiler with C++20 support (`g++ >= 11`, `clang++ >= 13`, or `MSVC 2019+`)
- SSE4.2 capable x86_64 CPU (for `_mm_crc32_u64` and `__rdtsc()`)

### Quick Build with Make / GCC
```bash
cd ULL-FLIG/sequencer

# Build daemon and test suite
make

# Run automated tests and benchmark
make test
```

### Build with CMake
```bash
mkdir build && cd build
cmake ..
cmake --build .
./test_sequencer
```

---

## Command-Line Options

```bash
./g12_sequencer [options]
  --inbound <path>   Inbound ring path (default: /dev/shm/g12-inbound.ring)
  --journal <path>   Journal ring path (default: /dev/shm/g12-journal.ring)
  --core <id>        CPU Core ID to pin thread to (default: 2)
  --no-pin           Disable core pinning
  --create-rings     Initialize ring files if not already present
  --help, -h         Show help message
```

---

## Performance Characteristics

* Measured on benchmark harness (`test_sequencer`):
  * **Throughput:** ~8.95 Million transactions / second.
  * **Latency p50:** ~114 CPU cycles (~25 to 30 ns).
  * **Latency p99:** ~228 CPU cycles (~50 to 60 ns).
  * **Heap Allocations on Hot Path:** **0 bytes** (zero dynamic memory allocation).
