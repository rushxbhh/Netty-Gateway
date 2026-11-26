# ULL-FLIG — External Examiner Q&A Reference

Prepared answers to likely questions from the external examiner panel. Each answer is concise enough to deliver verbally in 60–90 seconds and technically precise.

---

## System Design

### Q1. What problem does this project solve?

Financial transaction systems need two guarantees that are hard to combine: **low latency** at the ingress point (clients should not wait long for an acknowledgement) and **durable audit** (every transaction must be permanently recorded before it is considered finalized).

This project implements a **split-acknowledgement pipeline** that separates these two concerns. The Gateway immediately acknowledges receipt of a valid transaction (`ACCEPT|stage=INGRESS`) in under 1 ms, while a separate pipeline — C++ Sequencer → Java Audit → PostgreSQL — produces a durable commit confirmation (`ACK|stage=AUDITED|status=PERSISTED`) delivered back to the same WebSocket connection asynchronously. The client sees both stages; nothing is hidden.

---

### Q2. Why three separate services? Why not one process?

Three services reflect a real production split:

1. **Gateway (Java/Netty):** Handles WebSocket clients, protocol validation, and I/O multiplexing. It needs to be horizontally scalable and language-agnostic at the protocol boundary.
2. **Sequencer (C++):** Assigns monotonic sequence numbers at maximum speed with minimal GC jitter. C++ gives deterministic execution and direct hardware access (SSE4.2 CRC, cache-line-aligned structs).
3. **Audit (Java):** Runs business-rule validation, talks to PostgreSQL over JDBC, and benefits from Java's ecosystem for connection pooling and exception handling.

Separating them also means each can crash-restart independently without losing in-flight ring data.

---

### Q3. What is an SPSC ring buffer? Why use one instead of a concurrent queue?

**SPSC** = Single Producer, Single Consumer. It is a lock-free, wait-free bounded circular buffer where exactly one thread writes and exactly one thread reads.

Advantages over a concurrent (MPMC) queue:
- **No locks, no CAS loops** — the producer and consumer only synchronize via a single sequence counter. On x86, acquire/release loads/stores are sufficient; no `mfence` is needed.
- **Cache-line aligned slots** — each 64-byte slot fits exactly one cache line, preventing false sharing.
- **Predictable latency** — no lock contention means no unbounded wait. The worst case is a spin on an empty ring, not a mutex sleep.
- **Hardware-friendly** — prefetcher can predict sequential access patterns.

The constraint (one producer, one consumer) is acceptable here because each ring has exactly one producer and one consumer by design: Gateway → Inbound Ring → Sequencer → Journal Ring → Audit → ACK Ring → Gateway.

---

### Q4. What is CRC32C and why is it used here?

**CRC32C** (Castagnoli polynomial) is a 32-bit cyclic redundancy check. It is computed by the C++ Sequencer over the payload bytes of each 64-byte journal record and embedded in the record header.

The Java Audit service recomputes the CRC32C over the same bytes and compares it to the stored value. A mismatch means the record was corrupted in shared memory (e.g., partial write, bit flip) and the transaction is rejected.

We use CRC32C specifically (not CRC32b) because modern x86 CPUs expose a hardware instruction for it via SSE4.2 (`_mm_crc32_u64`). Our C++ implementation detects SSE4.2 at compile time and uses the intrinsic, achieving ~18 M ops/s. On non-x86 (Apple Silicon Docker), it falls back to a software implementation automatically via the `CMakeLists.txt` guard.

---

### Q5. What is FlatBuffers and why not use JSON?

**FlatBuffers** is a binary serialization format from Google. The Gateway receives WebSocket frames encoded as FlatBuffers.

Advantages over JSON for this use case:
- **Zero-copy access** — fields are read directly from the buffer via offsets; no parsing or object allocation.
- **Schema-enforced** — the `.fbs` schema defines exact field types. Invalid or missing fields are rejected before any business logic runs.
- **Compact** — a transaction frame is ~40 bytes; equivalent JSON is ~120 bytes.

JSON would add parsing overhead and allocations on every inbound message, increasing GC pressure on the Gateway's Netty I/O threads.

---

### Q6. Explain the two-stage ACK. Why does the distinction matter?

| Stage | Message | Meaning |
|---|---|---|
| Stage 1 | `ACCEPT\|stage=INGRESS` | Gateway validated the FlatBuffer and published it to the inbound ring. The transaction is in the pipeline but NOT yet durable. |
| Stage 2 | `ACK\|stage=AUDITED\|status=PERSISTED` | Audit confirmed CRC32C, sequence order, and PostgreSQL committed the row. The record is now durable. |

The distinction matters because a client that acts on Stage 1 alone might assume persistence that hasn't happened yet. A downstream account ledger, for example, should only credit/debit on Stage 2. Stage 1 is purely a pipeline health signal — it tells the client the message was received and is being processed, so they should not retry.

---

### Q7. What does "idempotent insert" mean and why is it needed?

The Audit service uses `INSERT INTO audit_log ... ON CONFLICT (transaction_id) DO NOTHING` (PostgreSQL upsert syntax).

If the Audit service crashes after committing to PostgreSQL but before writing the ACK to the ring, it will re-read the same journal record on restart and attempt to insert again. Without idempotency, this creates a duplicate row. With `ON CONFLICT DO NOTHING`, the second insert is silently discarded — the transaction is committed exactly once in the database regardless of how many times the Audit service processes that record.

This gives the system **at-least-once delivery** from the ring to the database, combined with **exactly-once storage** via the idempotent upsert — the standard pattern for reliable event processing.

---

### Q8. What is backpressure and how does the system handle it?

**Backpressure** is the mechanism by which a slow consumer signals to a fast producer to slow down, preventing ring buffer overflow.

In this system: if the C++ Sequencer's output journal ring is full (Audit is not consuming fast enough), the Sequencer's `publish()` call spins until a slot is free. This is **spin-wait backpressure** — the Sequencer blocks until Audit catches up. The Gateway, in turn, would see the inbound ring filling up and could reject new WebSocket messages with a `BACKPRESSURE` rejection reason.

The ring sizes are configured to absorb burst traffic without immediately hitting backpressure under normal load. Under sustained overload, the spin-wait ensures no data is silently dropped — the pipeline naturally throttles to the speed of the slowest stage (PostgreSQL commits, in practice).

---

## Performance

### Q9. What are your latency numbers and what do they mean?

From our benchmark (`PipelineLatencyBenchmark`, 300 total transactions):

| Phase | p50 Latency | Explanation |
|---|---|---|
| Ingress (Gateway → ring publish) | **~0.7–1.2 ms** | FlatBuffer decode + ring write |
| DB-Audit (ring → PostgreSQL commit) | **~185–200 ms** | Dominated by network RTT to Neon PostgreSQL in AWS us-east-2 |
| End-to-end (WebSocket send → PERSISTED ACK) | **~185–200 ms** | Effectively DB-Audit latency |

The ingress latency is sub-millisecond, confirming the Gateway and C++ Sequencer add negligible overhead. The database is the bottleneck, as expected for any system committing to a remote RDBMS.

---

### Q10. What is your throughput and why does it scale with concurrency?

| Scenario | Concurrency | Throughput |
|---|---|---|
| Sequential-100 | 1 | 5.19 tx/s |
| Concurrent-5 | 5 | 24.41 tx/s |
| Concurrent-10 | 10 | 50.66 tx/s |

**Why it scales:** The bottleneck is the PostgreSQL network round-trip (~190 ms). Sequential mode spends that 190 ms waiting per transaction. With 10 concurrent clients, the pipeline processes ~10 transactions in parallel across the same ~200 ms window, multiplying throughput ~10×. This is classic pipeline parallelism — the SPSC rings have no internal lock contention, so they do not become the bottleneck as concurrency increases.

---

### Q11. Why doesn't batch commit improve sequential-mode throughput?

Batch commit groups multiple audit records into one PostgreSQL transaction, reducing the number of network round-trips per record. However, in sequential mode there is only one in-flight transaction at a time — the next one is not submitted until the current one is ACKed. So there is nothing to batch. Batch commit only helps when multiple records arrive close together in time (concurrent mode), where the Audit service can drain several journal entries before committing.

---

### Q12. Why is the C++ Sequencer so fast (18+ M ops/s)?

Four reasons:
1. **No I/O:** The sequencer reads from and writes to memory-mapped files — no network, no disk.
2. **Cache-line aligned structs:** Each 64-byte slot is exactly one cache line. The CPU prefetcher handles sequential access efficiently.
3. **Hardware CRC32C:** `_mm_crc32_u64` from SSE4.2 computes the checksum in a single instruction per 8 bytes.
4. **No allocations:** The sequencer operates on raw `uint8_t*` pointers. There is no heap allocation in the hot path.

---

## Implementation Details

### Q13. How does the Gateway correlate the ACK back to the original WebSocket connection?

Each inbound transaction is assigned a `connectionId` (from the Netty `ChannelHandlerContext`) and a `transactionId` (from the FlatBuffer payload). The Gateway stores a `ConcurrentHashMap<transactionId, ChannelHandlerContext>` when it publishes to the inbound ring.

When the ACK ring reader (`AckRingConsumer`) reads a completed journal ACK, it extracts the `transactionId`, looks up the stored `ChannelHandlerContext`, and writes the final `ACK|stage=AUDITED` message to that specific WebSocket channel.

---

### Q14. What guarantees does the system provide?

| Guarantee | Mechanism |
|---|---|
| **At-least-once delivery to DB** | Audit re-reads ring from last committed sequence on restart |
| **Exactly-once storage** | `ON CONFLICT (transaction_id) DO NOTHING` idempotent upsert |
| **Monotonic sequencing** | Sequencer atomically increments a 64-bit counter; Audit rejects out-of-order records |
| **Integrity check** | CRC32C verified by Audit before any database write |
| **No silent data loss** | Ring backpressure blocks producers rather than dropping records |

The system does **not** guarantee at-most-once delivery to clients (a client may receive duplicate ACKs if the Gateway restarts). It does **not** implement a full two-phase commit — PostgreSQL commit is the final durability boundary.

---

### Q15. What would you do differently to scale this to production?

1. **Replace Neon (remote DB) with a local/co-located PostgreSQL** — this would cut E2E latency from ~190 ms to ~1–5 ms.
2. **Multiple Audit workers** — partition the journal ring by `transactionId % N` to run N parallel Audit consumers, each with its own DB connection pool.
3. **Persistent ring replay** — currently the ring files are reset on restart. A production version would use a write-ahead log or Kafka as a durable backing store.
4. **Gateway horizontal scaling** — route WebSocket connections through a load balancer; each Gateway instance gets its own inbound ring, with the Sequencer fan-in handled by a dispatcher.
5. **Formal SLA monitoring** — alert when p99 E2E latency exceeds a threshold, using the existing `/api/telemetry` endpoint as a data source.

---

### Q16. What is the memory ordering model for the SPSC ring?

The ring uses **acquire/release semantics**:
- The **producer** writes the slot payload, then stores the sequence counter with a **release store** (ensures all payload writes are visible before the counter increment).
- The **consumer** reads the sequence counter with an **acquire load** (ensures the payload bytes are visible before they are read).

On x86, this is naturally enforced by the strong memory model — regular stores are release stores and regular loads are acquire loads. No `mfence` or `lock` prefix is needed. On ARM (Apple Silicon Docker), explicit acquire/release annotations in the C++ `std::atomic` calls are necessary.

---

## Project-Specific

### Q17. Why not use Kafka or RabbitMQ instead of shared-memory rings?

Kafka and RabbitMQ introduce network hops even when all services run on the same machine. Our design goal is to minimize latency between Gateway and Sequencer — a network message broker would add tens of milliseconds per hop.

Shared-memory SPSC rings eliminate the network layer entirely: the producer writes to a memory-mapped file, the consumer reads from the same mapped region. The "transport" is a cache-line write, not a TCP packet. This is the same principle used in LMAX Disruptor and financial trading systems where microsecond latency matters.

Kafka would be appropriate if the services needed to run on separate machines or if replay/fan-out to multiple consumers was required — both are non-requirements for this single-machine demonstration.

---

### Q18. How do you handle the case where the Audit service is slow or down?

1. **Slow Audit:** The journal ring fills up. The C++ Sequencer's `publish()` call spin-waits on the ring until a slot is free. This applies natural backpressure to the entire pipeline — the inbound ring fills, and the Gateway begins rejecting new WebSocket connections with a `BACKPRESSURE` response. No data is dropped; throughput degrades gracefully.

2. **Audit crash:** The ring retains its contents (memory-mapped file, not volatile RAM). On restart, the Audit service reads `.pids.json` to find its last committed sequence and resumes from that position. Any records between the last commit and the crash are reprocessed. The idempotent upsert ensures no duplicates are inserted.

3. **Gateway health endpoint:** `GET /health` returns `"audit":"DEGRADED"` when the Audit process is not alive (detected via `ProcessHandle.isAlive(pid)`), giving operators immediate visibility.

---

### Q19. What does the frontend actually demonstrate?

The frontend at `http://localhost:8080/` is a live observable window into the pipeline. It demonstrates:

1. **Real-time pipeline progression:** The 5-stage stepper (Ingress → Sequencer → Journal → Audit → Persisted) advances as the actual ring messages flow, not on a timer.
2. **Service liveness:** The topbar shows live status of Sequencer, Audit, PostgreSQL, and WebSocket derived from the `/health` endpoint, not hardcoded.
3. **Architecture visualization:** An animated SVG diagram shows data flowing through the actual components, useful for explaining the design verbally during examination.
4. **Performance charts:** The Performance tab renders Chart.js charts from real benchmark data stored in `latency_benchmark_results.json` — not fabricated values.
5. **Validation:** Submitting amount=0 triggers a live rejection, demonstrating input validation at the Gateway level.
