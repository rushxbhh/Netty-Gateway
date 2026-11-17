# G12 Shared-Memory Ring Protocol

This document is the shared-memory contract between the Java Netty Gateway, C++ Sequencer, and Java Audit Service. The existing inbound and journal layouts are defined by `sequencer/include/sequencer/ring_layout.hpp`; the ACK ring is a compatible Java-defined extension.

## Ring header and counters

| Byte offset | Size | Meaning |
|---:|---:|---|
| 0 | 64 | Header cache line (`G12RING1`, version, slot size, capacity, ring kind) |
| 64 | 64 | Producer sequence cache line |
| 128 | 64 | Consumer sequence cache line |
| 192 | variable | Slot array |

The producer and consumer counters are 64-bit aligned. Each ring is single-producer/single-consumer. The inbound producer is the Java writer thread; its consumer is the C++ Sequencer. The journal producer is the C++ Sequencer; its consumer is the Java Audit Service. The Audit Service stores a non-zero journal `streamId` at header offset 32, within the header's reserved bytes. C++ ignores this field. It distinguishes a newly created volatile `/dev/shm` journal from a previous ring generation when PostgreSQL persists across Docker teardown/recreation.

## Inbound ring: Java Gateway -> C++ Sequencer

| Property | Value |
|---|---:|
| Slot size | 128 bytes |
| Capacity | 65,536 slots |
| Metadata per slot | 32 bytes |
| Maximum FlatBuffer payload | 96 bytes |
| Slot array starts | byte 192 |

Inbound slot layout:

| Slot-relative offset | Size | Field |
|---:|---:|---|
| 0 | 4 | `payloadLen` |
| 4 | 4 | `flags` |
| 8 | 8 | `connectionId` |
| 16 | 8 | `clientTxnId` validated by Gateway; used to correlate parser-reject ACKs |
| 24 | 8 | Reserved, zero |
| 32 | 96 | FlatBuffer payload bytes |

The 96-byte limit applies to the complete FlatBuffer byte sequence, including its root offset and `NIGT` file identifier. It is not a limit on the transaction struct alone.

## Journal ring: C++ Sequencer -> Java Audit Service

| Property | Value |
|---|---:|
| Slot size | 64 bytes |
| Capacity | 1,048,576 records |
| Slot array starts | byte 192 |

The 64-byte `JournalSlot` is a separate, fixed output record containing the sequencer sequence ID, timestamp counter, client transaction ID, connection/account fields, amount, type, status and CRC32C checksum.

**There is no conflict between 96 and 64:** inbound payload bytes and downstream journal record bytes belong to different rings and have different layouts.

## ACK ring: Java Audit Service -> Java Gateway

| Property | Value |
|---|---:|
| Ring kind | 3 |
| Slot size | 64 bytes |
| Capacity | 65,536 records |
| File size | 4,194,496 bytes |

ACK slot layout:

| Slot-relative offset | Size | Field |
|---:|---:|---|
| 0 | 8 | Journal sequence ID |
| 8 | 8 | Client transaction ID |
| 16 | 8 | Gateway connection ID |
| 24 | 8 | Audit time (Unix epoch milliseconds) |
| 32 | 4 | Audit status (`1 = PERSISTED`) |
| 36 | 4 | Original journal status (`0 = OK`, `1 = malformed`) |
| 40 | 20 | Reserved, zero |
| 60 | 4 | CRC32C over bytes 0..59 |

Audit owns ACK `producerSeq`; Gateway owns ACK `consumerSeq`. The ACK ring uses the same header and counter offsets as the other rings. Gateway advances its ACK consumer after the ACK has been routed to the original Netty event loop and the WebSocket write has completed, or the originating connection no longer exists.

## Startup and ownership

- Each ring file is initialized only when its file is empty.
- Existing non-empty files are validated and attached without resetting counters or slots.
- Existing non-empty files smaller than the required layout are rejected as truncated.
- Java `FileChannel.lock()` and the C++ startup file lock protect the initialization/validation critical section.
- Runtime ownership remains SPSC. Do not run multiple gateway writer threads or multiple gateway replicas against one inbound ring.

## File sizes

The expected minimum mapping sizes are calculated as `192 + capacity * slotSize`:

- Inbound: 8,388,800 bytes.
- Journal: 67,109,056 bytes.
- ACK: 4,194,496 bytes.
