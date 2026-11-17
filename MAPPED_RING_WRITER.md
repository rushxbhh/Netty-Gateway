# Java inbound shared-memory ring writer

`MappedSpscRingWriter` connects the Netty gateway to the C++ sequencer through
`/dev/shm/g12-inbound.ring` (or a configured path). It implements the inbound
ring contract in `sequencer/include/sequencer/ring_layout.hpp`.

## Ownership and threads

Netty may execute handlers for different WebSocket channels on different event
loop threads. The C++ ring is SPSC, so multiple Netty threads must not update
`producerSeq` directly.

`MappedSpscRingWriter` therefore has two layers:

1. `submit()` is safe to call from multiple Netty event loops. It copies the
   payload into owned memory and offers it to a bounded `ArrayBlockingQueue`.
2. One dedicated `g12-inbound-ring-writer` thread drains that queue and is the
   only thread that writes ring slots or updates the ring's `producerSeq`.

This preserves a single logical Java producer for the C++ SPSC ring. The
bounded queue prevents unlimited memory growth. Queue saturation returns
`QUEUE_FULL`; a ring that stays full for two seconds returns `RING_FULL`.

## Shared file layout

All integer fields use little-endian encoding on a little-endian host.

| File offset | Size | Meaning |
|---:|---:|---|
| 0 | 64 | Ring header (`G12RING1`, version, slot size, capacity, ring kind) |
| 64 | 8 | `producerSeq`, owned by Java |
| 128 | 8 | `consumerSeq`, owned by C++ |
| 192 | 8,388,608 | 65,536 inbound slots × 128 bytes |

The 64-byte `JournalSlot` belongs to the separate downstream journal ring. It
is not the inbound payload limit. Each 128-byte inbound slot is:

| Slot offset | Size | Meaning |
|---:|---:|---|
| 0 | 4 | FlatBuffers payload length (1–96) |
| 4 | 4 | Flags (currently zero) |
| 8 | 8 | Connection ID |
| 16 | 8 | Gateway-validated client transaction ID |
| 24 | 8 | Reserved (zero) |
| 32 | 96 | FlatBuffers payload area |

The Gateway writes the validated transaction ID into the metadata as well as
keeping it in the FlatBuffer. This lets the Sequencer preserve the correlation ID
if its parser rejects a payload that passed Gateway validation.

The complete mapped file is 8,388,800 bytes: 192 bytes of control metadata plus
8 MiB of slots.

## Publication protocol

For producer sequence `p`, Java computes:

```text
slotIndex  = p & (capacity - 1)
slotOffset = 192 + slotIndex * 128
```

The writer then:

1. Reads its own `producerSeq`.
2. Reads C++ `consumerSeq` with acquire semantics before reusing a slot.
3. Checks `producerSeq - consumerSeq < capacity`.
4. Writes `payloadLen`, flags, connection ID, reserved bytes and payload.
5. Publishes `producerSeq = p + 1` with `VarHandle.setRelease`.

The C++ sequencer reads `producerSeq` with `memory_order_acquire`. It therefore
checks the published producer counter before reading the slot. After it has
finished processing the slot and publishing its journal record, it advances
`consumerSeq` with `memory_order_release`. Java reads that counter with acquire
semantics before it reuses the slot.

Never publish `producerSeq` before the slot contents are complete.

## Gateway response meaning

The handler replies only after the writer has published the inbound slot:

```text
ACCEPT|stage=INGRESS|transactionId=...|inboundSequence=...
```

`inboundSequence` identifies the inbound ring slot. It is **not** the journal
sequence assigned later by C++. This response does not mean that the transaction
has been sequenced, persisted, or audited.

## Configuration

Default path:

- `/dev/shm/g12-inbound.ring` when `/dev/shm` exists and is writable.
- `./g12-inbound.ring` otherwise (local development fallback).

Override with either:

```bash
G12_INBOUND_RING_PATH=/dev/shm/g12-inbound.ring
```

or:

```bash
java -Dg12.inbound.ring=/dev/shm/g12-inbound.ring ...
```

The staging queue defaults to 4,096 entries and can be changed with
`-Dg12.inbound.queue-capacity=8192`.

## Initialization and restart safety

An empty file is sized and initialized while holding an exclusive startup file
lock. Java uses `FileChannel.lock()`; the C++ `MmapFile` startup path uses the
compatible OS file lock. This prevents both processes from initializing the
same empty ring concurrently.

The C++ sequencer now handles each ring independently:

- It initializes the inbound ring only if the inbound file is empty; otherwise
  it validates and attaches without resetting counters or slots.
- It initializes the journal ring only if the journal file is empty; otherwise
  it validates and attaches without resetting counters or slots.
- A non-empty but undersized file is rejected as truncated instead of being
  silently expanded.

The old `--create-rings` CLI flag is retained as a deprecated no-op for older
commands. Existing ring files are never reset by that flag. To intentionally
reset a development ring, stop every producer and consumer process first, then
remove the relevant ring file manually.

## Current boundaries

- One Gateway process may own this inbound SPSC ring. Multiple Gateway replicas
  cannot share the same SPSC producer endpoint; use one ring per replica or
  redesign the protocol for multiple producers.
- The first implementation uses `ArrayBlockingQueue` for correctness and
  bounded backpressure. It can later be replaced with a specialized MPSC queue
  after correctness and latency measurements.
- The shared-memory protocol requires a little-endian host and lock-free
  aligned 64-bit atomic operations. The cross-process Java/C++ protocol has been
  smoke-tested on Linux x86_64; repeat the same interoperability test on ARM64
  before production deployment.


## Final audit acknowledgement path

The Gateway now keeps a pending request keyed by `(connectionId, transactionId)`
after ingress registration. The separate Audit Service consumes the 64-byte
journal records and writes an ACK to `g12-ack.ring` after PostgreSQL commit. The
ACK contains journal sequence, transaction ID, connection ID, audit status, and
CRC32C. The Gateway ACK reader routes it to the pending Netty channel and sends
`ACK|stage=AUDITED|...` after the earlier `ACCEPT|stage=INGRESS|...` response.

Audit owns ACK `producerSeq`; Gateway owns ACK `consumerSeq`. Audit only advances
`journal.consumerSeq` after its database transaction and ACK publication.
