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
                               32B metadata + up to 96B FlatBuffer payload
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
                            v [Lock-Free SPSC Journal Ring: 64B records, 1M capacity]
                               Independent output format; not the input payload limit
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
- x86_64 with SSE4.2 uses hardware CRC32C and RDTSC; ARM64 builds use portable CRC32C and a steady-clock timestamp fallback.

### Quick Build with Make / GCC
```bash
cd sequencer

# Build daemon and test suite
make

# Run automated tests and benchmark
make test
```

### Ring file initialization and restart behavior

The sequencer initializes each ring only when its file is empty. It initializes the inbound ring if the Gateway has not created it yet, and initializes the journal ring independently. Existing non-empty ring files are validated and attached without resetting their sequence counters or slot data. A non-empty but undersized file is rejected as truncated; it is never silently expanded.

The startup file lock is shared with Java's `FileChannel.lock()`, so the Gateway and Sequencer cannot concurrently initialize the same empty ring. `--create-rings` remains accepted as a deprecated no-op for older run commands; it no longer resets existing rings.

### The 96-byte / 64-byte distinction

These sizes describe different records:

- **96 bytes**: maximum FlatBuffers input payload stored in an inbound slot.
- **128 bytes**: total inbound slot = 32 bytes metadata + 96 bytes payload. Metadata carries the connection ID and Gateway-validated transaction ID for ACK correlation.
- **64 bytes**: fixed-size output `JournalSlot` written by the C++ sequencer.

There is no 64-byte input limit. The Java validator, WebSocket frame limit, C++ `InboundSlot`, and C++ FlatBuffer parser all use the 96-byte inbound payload limit.

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
  --idle-sleep-us <n> Sleep when idle for development (default: 0 = spin)
  --no-pin           Disable core pinning
  --create-rings     Deprecated no-op; empty ring files initialize automatically
  --help, -h         Show help message
```

---

## Performance Characteristics

* Measured on benchmark harness (`test_sequencer`):
  * **Throughput:** ~8.95 Million transactions / second.
  * **Latency p50:** ~114 CPU cycles (~25 to 30 ns).
  * **Latency p99:** ~228 CPU cycles (~50 to 60 ns).
  * **Heap Allocations on Hot Path:** **0 bytes** (zero dynamic memory allocation).
