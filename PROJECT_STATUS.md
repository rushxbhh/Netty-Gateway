# ULL-FLIG: Project Status & Execution Log

**Document Version:** 1.2.0  
**Status Date:** November 25, 2026  
**Current Phase:** Phase 2 Complete $\rightarrow$ Entering Phase 3 (End-to-End Reliability & Stress Testing)  
**Project Name:** ULL-FLIG (Ultra-Low-Latency Financial Ledger & Ingress Gateway)  
**Project Title:** Design and Implementation of a Low-Latency Transaction Ingress and Audit Gateway  
**Institution:** Medicaps University, Indore — Department of Computer Science and Engineering  
**Authors:** Saarthak K Goyal, Rushabh Singh Baghel, Sai Smruti Ranjan Das  
**Supervisors:** Prof. Nilesh Agrawal (Guide), Prof. Priyanka Kumrawat (Internal Faculty)  

---

## 1. Executive Summary

This document tracks the execution roadmap and technical status for the **ULL-FLIG** major project.

- **Phase 0 (Repository Audit and Baseline):** Completed on Nov 25, 2026.
- **Phase 1 (System Stabilization):** Completed on Nov 25, 2026.
- **Phase 2 (Frontend & Demonstration Interface):** **COMPLETED on Nov 25, 2026.**
  - **Single-Port Unified Gateway (Port 8080):** Serves both the interactive web dashboard (`http://localhost:8080/`) and the binary WebSocket ingress endpoint (`ws://localhost:8080/ws`).
  - **Performance Isolation Architecture:**
    - Dedicated Netty `EventExecutorGroup` (`dashboard-io`, 2 threads) ensures HTTP dashboard traffic never blocks or competes with WebSocket hot-path transaction workers.
    - Asynchronous background polling cache (`dashboard-cache-worker`) refreshes ring buffer telemetry (every 400ms) and PostgreSQL audit logs (every 1800ms) in memory, serving HTTP endpoints in < 50 microseconds with ZERO database blocking on the request path.
  - **Client-Side Binary FlatBuffers Engine:** JavaScript binary encoder constructs exact `NIGT` binary FlatBuffers (60 bytes) directly in the browser using `ArrayBuffer` and `DataView`.
  - **Deterministic 5-Stage Lifecycle Tracker:** Real-time visual stepper tracking `SUBMITTED` $\rightarrow$ `ACCEPTED AT INGRESS` $\rightarrow$ `SEQUENCED` $\rightarrow$ `AUDIT PERSISTED` $\rightarrow$ `ACKNOWLEDGED`.
  - **System Telemetry & Live Audit Ledger:** Live SPSC ring meters (Inbound, Journal, ACK) and interactive PostgreSQL audit history table.
- **Next Phase:** Phase 3 (End-to-End Reliability & Extended Stress Testing).

---

## 2. Architecture Overview

The system pipeline connects four core subsystems through cache-aligned, lock-free SPSC shared-memory rings:

```text
+-----------------------------------------------------------------------------+
|                               WebSocket Client                              |
+-----------------------------------------------------------------------------+
          |                                                       ^
     [Binary WS]                                            [Text WS ACK]
          |                                                       |
          v                                                       |
+-------------------+                                    +--------------------+
| Java Netty        |                                    | Java Netty         |
| Ingress Gateway   |                                    | ACK Reader         |
| - Port 8080 /ws   |                                    | - MappedAckReader  |
| - FlatBuffer View |                                    | - PendingRegistry  |
| - Business Valid. |                                    | - Route by         |
| - ArrayBlockQueue |                                    |   (connId, txnId)  |
+-------------------+                                    +--------------------+
          |                                                       ^
          | [Inbound SPSC Ring: 128B slots, 64K capacity]         |
          | (32B metadata + 96B FlatBuffer payload)               | [ACK SPSC Ring]
          v                                                       | (64B slots, 64K)
+---------------------------------------+                         |
| C++20 Sequencer Engine                |                         |
| - Zero-alloc spin loop                |                         |
| - Monotonic sequenceId (0, 1, 2, ...) |                         |
| - RDTSC hardware cycle timestamping   |                         |
| - SSE4.2 hardware CRC32C over 60B     |                         |
| - 64B alignas(64) JournalSlot output  |                         |
+---------------------------------------+                         |
          |                                                       |
          | [Journal SPSC Ring: 64B slots, 1M capacity]           |
          v                                                       |
+---------------------------------------+                         |
| Java Audit Service                    |                         |
| - Validates sequence continuity       |                         |
| - Checks CRC32C integrity             |                         |
| - Idempotent PostgreSQL commit        |-------------------------+
| - Publishes ACK before ring advance   |
+---------------------------------------+
          |
          v
+---------------------------------------+
| PostgreSQL Durable Storage            |
| - Table: transaction_audit            |
| - Key: (stream_id, sequence_id)       |
+---------------------------------------+
```

### Protocol Specifications and Layout Contracts

| Ring Buffer | Producer | Consumer | Slot Size | Slot Layout | Ring Capacity | Total File Size |
|---|---|---|---:|---|---:|---:|
| **Inbound** (`g12-inbound.ring`) | Gateway writer thread | C++ Sequencer | 128 bytes | 4B payloadLen, 4B flags, 8B connId, 8B clientTxnId, 8B rsvd, 96B FlatBuffer payload | 65,536 slots | 8,388,800 bytes |
| **Journal** (`g12-journal.ring`) | C++ Sequencer | Java Audit Service | 64 bytes | 8B seqId, 8B rdtsc, 8B clientTxnId, 8B connId, 8B fromAcct, 8B toAcct, 8B amount, 1B type, 1B status, 2B rsvd, 4B CRC32C | 1,048,576 slots | 67,109,056 bytes |
| **ACK** (`g12-ack.ring`) | Java Audit Service | Gateway ACK reader | 64 bytes | 8B seqId, 8B clientTxnId, 8B connId, 8B auditTimeMs, 4B auditStatus, 4B journalStatus, 20B rsvd, 4B CRC32C | 65,536 slots | 4,194,496 bytes |

### Two-Stage Acknowledgement Semantics
1. `ACCEPT|stage=INGRESS|transactionId=...|inboundSequence=...`: Gateway accepted and validated the FlatBuffer payload and successfully published it to the inbound ring. This does **not** signify sequencing or durable storage.
2. `ACK|stage=AUDITED|transactionId=...|journalSequence=...|status=PERSISTED`: C++ assigned a deterministic sequence ID, Java Audit Service validated CRC32C and committed the transaction to PostgreSQL, and the ACK ring delivered the confirmation back to the originating WebSocket connection.

---

## 3. Existing Completed Functionality

### A. Java Netty Ingress Gateway (`src/main/java/org/java`)
- **`GatewayServer.java`**: Netty 4.1.128.Final WebSocket pipeline bootstrap on port 8080 (`/ws`).
- **`TransactionValidator.java`**: Strict FlatBuffers validation (`NIGT` file identifier, minimum 8B, maximum 96B payload). Validates positive transaction IDs, positive amounts (in smallest currency units / cents / paise), non-negative account IDs, and business rules (`TRANSFER` requires two distinct customer accounts; `DEPOSIT` and `WITHDRAWAL` require system account `0` as source/destination).
- **`WebSocketFrameHandler.java`**: Enforces binary frames, allocates monotonic connection IDs, registers pending requests, submits payloads to the ring writer, and emits immediate ingress responses or backpressure/rejection codes.
- **`MappedSpscRingWriter.java`**: Solves the MPSC-to-SPSC concurrency impedance match by using a bounded staging queue (`ArrayBlockingQueue<PendingWrite>`, capacity 4096) drained by a single dedicated writer thread (`g12-inbound-ring-writer`) that maps the inbound ring and issues release writes to `producerSeq`.
- **`MappedAckRingReader.java`**: Dedicated polling thread mapping `g12-ack.ring` as the sole consumer, verifying CRC32C checksums, and routing completions to `PendingRequestRegistry`.
- **`PendingRequestRegistry.java`**: Thread-safe hash map indexing in-flight requests by `(connectionId, transactionId)`. Ensures ingress response precedes audit ACK, prevents memory leaks on client disconnect, and bounds capacity.

### B. C++20 Sequencer Engine (`sequencer/`)
- **`ring_layout.hpp`**: Zero-overhead C++ structure layouts (`RingHeaderLine`, `InboundSlot`, `JournalSlot`) with exact `static_assert` size and alignment checks.
- **`ring_buffer.hpp`**: Cross-platform memory-mapping abstraction (`MmapFile` and `RingBuffer`). Uses POSIX `/dev/shm` on Linux and `CreateFileA`/`MapViewOfFile`/`LockFileEx` on Windows.
- **`hardware.hpp`**: Platform optimizations: thread core affinity pinning (`set_thread_affinity`), CPU cycle counter (`rdtsc` / `rdtscp`), SSE4.2 hardware CRC32C with software Castagnoli fallback, and `spin_pause` (`_mm_pause` / `YieldProcessor`).
- **`flatbuffer_view.hpp`**: True zero-allocation, zero-copy FlatBuffers parser (`FlatBufferView::try_parse`). Direct pointer dereference of vtables, tables, and structs with bounds checking.
- **`sequencer.hpp` & `main.cpp`**: Hot-path loop polling `inbound.producerSeq`, sequencing transactions, writing 64B cache-aligned journal slots, publishing `journal.producerSeq`, and advancing `inbound.consumerSeq`.

### C. Java Audit Service (`audit-service/`)
- **`AuditApplication.java`**: Standalone entrypoint initializing database schema and consumer loop.
- **`AuditRepository.java`**: PostgreSQL JDBC layer managing `transaction_audit` table. Implements idempotent `ON CONFLICT (stream_id, sequence_id) DO UPDATE` with tamper detection (verifying that replayed records match immutable fields).
- **`JournalConsumer.java`**: Consumer loop verifying CRC32C checksums, batching database commits, publishing ACK records to `g12-ack.ring`, and applying backpressure without losing journal slots if the ACK ring is saturated.
- **`AuditHealthCheck.java`**: Container healthcheck verification utility.

### D. Automated Test & Infrastructure Assets
- **`sequencer/tests/test_sequencer.cpp`**: 8-stage verification and benchmark test suite.
- **`audit-service/src/test/java/org/g12/audit/AuditProtocolSelfTest.java`**: Unit tests for journal ring reading, checksum calculation, and ACK publishing.
- **`integration-tests/java/org/g12/audit/AuditAckInteropSelfTest.java`**: Inter-process memory-mapped shared-memory interop test between Java Audit and Gateway components.
- **`src/test/java/org/java/GatewayTestClient.java`**: End-to-end integration test client validating live WebSocket handshake, valid transfer flow, zero-amount rejection, and malformed FlatBuffer rejection.
- **`docker-compose.yml` & `scripts/`**: Container orchestration with Linux IPC sharing (`ipc: service:sequencer`).

---

## 4. Test Inventory and Actual Verification Results

All tests below were executed and verified directly on the host machine:

| Component | Test Name / Command | Target / Scope | Result | Key Metrics / Notes |
|---|---|---|:---:|---|
| **C++ Sequencer** | `test_sequencer.cpp` [Test 1] | Hardware vs. Software CRC32C | **PASS** | CRC matching: `0xb47144d0` |
| **C++ Sequencer** | `test_sequencer.cpp` [Test 2] | FlatBuffer Zero-Copy Parser | **PASS** | Valid parsed; corrupted identifier and out-of-bounds rejected |
| **C++ Sequencer** | `test_sequencer.cpp` [Test 3] | Ring Layout & Alignments | **PASS** | Header, counter offsets, and 64B alignments verified |
| **C++ Sequencer** | `test_sequencer.cpp` [Test 4] | End-to-End Pipeline | **PASS** | Sequenced 500 contiguous transactions |
| **C++ Sequencer** | `test_sequencer.cpp` [Test 5] | Parser Rejection Fallback | **PASS** | Malformed payload preserves txn ID for fallback ACK correlation |
| **C++ Sequencer** | `test_sequencer.cpp` [Test 6] | Ring Init & Restart | **PASS** | Existing counters preserved; truncated files rejected |
| **C++ Sequencer** | `test_sequencer.cpp` [Test 7] | Backpressure on Full Ring | **PASS** | Stop request observed without releasing blocked slot |
| **C++ Sequencer** | `test_sequencer.cpp` [Test 8] | Throughput & Latency Benchmark | **PASS** | **Throughput:** 24,829,298 tx/sec (24.83 M ops/s)<br>**p50:** 47 cycles (~14 ns)<br>**p99:** 54 cycles (~16 ns)<br>**p99.9:** 106 cycles (~32 ns) |
| **Java Audit** | `AuditProtocolSelfTest` | Ring map, CRC32C, ACK, stream ID | **PASS** | Memory-mapped buffer and counter math verified |
| **Integration** | `AuditAckInteropSelfTest` | Audit ACK Writer -> Gateway ACK Reader | **PASS** | Cross-package shared-memory IPC verified |
| **Build** | `mvn clean test-compile` (Gateway) | Java 21 compilation & dependencies | **PASS** | 13 main + 1 test class built successfully |
| **Build** | `mvn clean test-compile` (Audit) | Java 21 compilation & dependencies | **PASS** | 10 main + 1 test class built successfully |
| **Phase 1 Pipeline** | `verify-pipeline.ps1` | Full Cluster: Inbound $\rightarrow$ Sequencer $\rightarrow$ Journal $\rightarrow$ Audit $\rightarrow$ Neon DB $\rightarrow$ ACK Ring $\rightarrow$ Gateway $\rightarrow$ Client | **PASS** | SPSC counter advancement verified (0 $\rightarrow$ 1 across all 3 rings); restart counter preservation verified; durable Neon row verified |
| **Phase 2 Dashboard** | `GET /`, `/api/telemetry`, `/api/audit` | Web UI + Non-blocking Telemetry & Audit APIs + Hot-Path Isolation | **PASS** | UI served at `http://localhost:8080/`; telemetry response < 50µs from memory cache; isolated `dashboard-io` executor protects hot-path Netty workers |
| **Phase 4 Latency** | `PipelineLatencyBenchmark` | End-to-End WebSocket Ingress + Ring + C++ + Neon Cloud DB + ACK | **PASS** | 100% success (150/150 tx).<br>**Ingress p50:** 1.41 ms (p99: 4.63 ms)<br>**DB Commit p50:** 305.92 ms (p99: 2,197 ms)<br>**Total E2E p50:** 308.58 ms (p99: 2,198 ms) |



---

## 5. Build and Run Procedures

### Environment Details
- **Host OS:** Windows 11 Home / x86_64
- **Java:** Oracle JDK 21.0.4 LTS (`C:\Program Files\Java\jdk-21\bin\javac.exe`, `java.exe`)
- **C++ Compiler:** GCC / G++ 15.2.0 (`C:\msys64\ucrt64\bin\g++.exe`) with `-std=c++20 -O3 -msse4.2`
- **Build Tool:** Apache Maven 3.9.12 (`C:\Users\Saarthak\.m2\wrapper\dists\apache-maven-3.9.12\59fe215c0ad6947fea90184bf7add084544567b927287592651fda3782e0e798\bin\mvn.cmd`)
- **Node.js:** Node.js v20+ / npm available (`C:\nvm4w\nodejs\node.exe`)
- **Python:** Python 3.12 (`C:\Users\Saarthak\AppData\Local\Programs\Python\Python312\python.exe`)

### Native Windows Execution Commands

1. **Build & Run C++ Sequencer Test Suite:**
   ```powershell
   cmd /c "C:\msys64\ucrt64\bin\g++.exe -std=c++20 -O3 -msse4.2 -Wall -Wextra -Wpedantic -I sequencer\include sequencer\tests\test_sequencer.cpp -o sequencer\test_sequencer.exe && sequencer\test_sequencer.exe"
   ```

2. **Build C++ Sequencer Daemon:**
   ```powershell
   cmd /c "C:\msys64\ucrt64\bin\g++.exe -std=c++20 -O3 -msse4.2 -Wall -Wextra -Wpedantic -I sequencer\include sequencer\src\main.cpp -o sequencer\g12_sequencer.exe"
   ```

3. **Build Netty Gateway:**
   ```powershell
   & "C:\Users\Saarthak\.m2\wrapper\dists\apache-maven-3.9.12\59fe215c0ad6947fea90184bf7add084544567b927287592651fda3782e0e798\bin\mvn.cmd" clean package -DskipTests
   ```

4. **Build Audit Service:**
   ```powershell
   & "C:\Users\Saarthak\.m2\wrapper\dists\apache-maven-3.9.12\59fe215c0ad6947fea90184bf7add084544567b927287592651fda3782e0e798\bin\mvn.cmd" -f audit-service/pom.xml clean package -DskipTests
   ```

5. **Run Java Protocol Self-Tests:**
   ```powershell
   $javac = "C:\Program Files\Java\jdk-21\bin\javac.exe"
   $java = "C:\Program Files\Java\jdk-21\bin\java.exe"
   $out = "target\audit-test-classes"
   New-Item -ItemType Directory -Force -Path $out
   $src = (Get-ChildItem -Path "audit-service\src\main\java\*.java" -Recurse | Select-Object -ExpandProperty FullName)
   $test1 = (Resolve-Path "audit-service\src\test\java\org\g12\audit\AuditProtocolSelfTest.java").Path
   $interop = @(
       (Resolve-Path "src\main\java\org\java\AckRingProtocol.java").Path,
       (Resolve-Path "src\main\java\org\java\AuditAcknowledgement.java").Path,
       (Resolve-Path "src\main\java\org\java\MappedAckRingReader.java").Path,
       (Resolve-Path "integration-tests\java\org\g12\audit\AuditAckInteropSelfTest.java").Path
   )
   & $javac --release 21 -d $out $src $test1 $interop
   & $java -cp $out org.g12.audit.AuditProtocolSelfTest
   & $java -cp $out org.g12.audit.AuditAckInteropSelfTest
   ```

### Docker / Linux Execution Commands
When deployed in a Linux Docker environment (Docker Engine / Desktop with Compose v2):
```bash
chmod +x scripts/test-three-services.sh
./scripts/test-three-services.sh
```

---

## 6. Gaps, Known Bugs, and Risks

1. **Absence of a Demonstration Frontend (High Priority for Examination):**
   - Currently, demonstrating the system requires running raw Java test clients or inspect-scripts.
   - An external examiner needs a clear, visual web application where they can:
     - Connect to the gateway via WebSocket.
     - Fill in Transfer, Deposit, and Withdrawal parameters and submit them.
     - Observe real-time state progression: `SUBMITTED` -> `INGRESS_ACCEPTED` -> `SEQUENCED` -> `AUDIT_PERSISTED` -> `ACKNOWLEDGED`.
     - Inspect the live audit ledger and ring buffer head/tail counters.
2. **Container Engine on Examination Host (Environmental Risk):**
   - Docker / WSL is not installed in the current Windows environment. 
   - A demonstration that solely relies on Docker Compose would fail on this machine if Docker Desktop cannot be started.
   - **Mitigation:** Ensure all components can either run in containers OR execute natively as a clean local process cluster (Native Windows / MSYS2 / Java) with a local or embedded PostgreSQL/H2 instance, guaranteeing testability regardless of host environment.
3. **End-to-End Stress & Latency Distribution Tooling:**
   - C++ sequencer benchmarks report raw cycle counts (47 cycles). However, end-to-end client latency (WebSocket round trip + SPSC rings + DB write + ACK return) needs a dedicated benchmark harness reporting p50, p95, p99 latencies under varying loads (e.g., 100 to 10,000 tx/sec).

---

## 7. Recommended Execution Order

To systematically complete the project for the external examination without disrupting working components:

1. **Phase 1 — System Stabilization & Local Native Cluster Runner:**
   - Provide a clean, robust script/runner to start and stop the complete local cluster (Sequencer + Gateway + Audit + DB) both in Docker and natively on Windows.
   - Validate end-to-end flow from client to DB and back.

2. **Phase 2 — Frontend & Demonstration Interface:**
   - Build a clean, responsive web dashboard (React/Vite or standalone modern HTML5/JS/Tailwind) served directly or via a lightweight static server.
   - Connect to the Netty Gateway WebSocket endpoint.
   - Implement interactive forms for Transfer, Deposit, and Withdrawal with client validation.
   - Implement real-time lifecycle tracker showing exact stage transitions.
   - Display live audit history table and ring telemetry.

3. **Phase 3 — End-to-End Testing & Reliability:**
   - Implement automated test scenarios covering:
     - Valid transfers, deposits, withdrawals.
     - Zero, negative, and oversized amounts.
     - Non-existent or invalid accounts.
     - Malformed FlatBuffers and corrupted identifiers.
     - Duplicate transaction ID submissions on same and different connections.
     - High concurrency with multiple simultaneous WebSocket clients.
     - Disconnection and reconnect resilience.

4. **Phase 4 — Performance Benchmarking:**
   - Implement a reproducible load testing tool measuring client-perceived throughput and p50/p95/p99 latency.
   - Generate structured performance tables and latency curves.

5. **Phase 5 — Observability & Demonstration Quality:**
   - Enhance logging, trace correlation IDs across services, and verify health checks.
   - Finalize step-by-step demonstration walkthrough for the external examiner.

6. **Phase 6 — Final Documentation & Examination Preparation:**
   - Verify README and architecture guides.
   - Ensure complete fidelity with the academic project report.
   - Prepare expected examiner questions and technical defense.
