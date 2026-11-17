# G12 Gateway + Sequencer + Audit: Linux/Docker Setup

This project runs three Java/C++ services as Linux containers. Docker Compose shares one Linux IPC namespace so all processes map the same `/dev/shm` ring files.

## Architecture

```text
WebSocket client
      |
      v
Java Netty Gateway -- g12-inbound.ring --> C++ Sequencer
      ^                                       |
      |                                       | g12-journal.ring
      |                                       v
      +---------- g12-ack.ring -------- Java Audit Service
                                              |
                                              v
                                          PostgreSQL
```

The three rings are SPSC:

| Ring | Producer | Consumer | Slot size | Capacity |
|---|---|---|---:|---:|
| Inbound | Gateway writer thread | C++ Sequencer | 128 B | 65,536 |
| Journal | C++ Sequencer | Java Audit | 64 B | 1,048,576 |
| ACK | Java Audit | Gateway ACK reader | 64 B | 65,536 |

The 96-byte inbound FlatBuffer maximum does not conflict with the 64-byte journal/ACK record sizes. They are separate rings and layouts.

The Sequencer owns a 128 MiB shareable IPC namespace. Audit and Gateway join it with `ipc: service:sequencer`. PostgreSQL has a named volume so audit rows survive container recreation.

## Prerequisites

- Docker Desktop with Linux containers enabled, or Docker Engine on Linux.
- Docker Compose v2.
- At least 2 CPU cores and 2 GB Docker memory for builds and tests.

No local Maven or C++ compiler is required for the Docker build.

## Run the complete integration test

From the repository root:

```bash
chmod +x scripts/test-three-services.sh
./scripts/test-three-services.sh
```

The script builds and starts the Sequencer, PostgreSQL, Audit Service and Gateway; waits for health checks; verifies that the services see the same IPC files; sends WebSocket transactions; checks the journal and ACK counters; and queries PostgreSQL for the persisted test transfer.

Expected test flow for the valid transfer:

```text
ACCEPT|stage=INGRESS|transactionId=1001|inboundSequence=...
ACK|stage=AUDITED|transactionId=1001|journalSequence=...|status=PERSISTED
```

Invalid-amount and malformed-FlatBuffer requests are rejected by Gateway and do not enter the rings.

## Inspect services

```bash
docker compose ps
docker compose logs -f sequencer audit gateway
```

Inspect shared files from any container in the shared IPC namespace:

```bash
docker compose exec sequencer ls -lh /dev/shm
```

Expected files:

- `g12-inbound.ring` — 8,388,800 bytes.
- `g12-journal.ring` — 67,109,056 bytes.
- `g12-ack.ring` — 4,194,496 bytes.

## Acknowledgement semantics

Gateway sends `ACCEPT|stage=INGRESS` after it publishes to the inbound ring. It registers the pair `(connectionId, transactionId)` before publication. Audit verifies the journal record, commits it to PostgreSQL, publishes an ACK record, and then releases the journal consumer counter. Gateway reads the ACK ring and delivers `ACK|stage=AUDITED` to the matching WebSocket connection.

The Gateway sends the ingress response before the final audit response, even if Audit is fast and publishes its ACK before the ingress callback runs. A disconnected client cannot receive a WebSocket response; the audit row remains persisted.

`PERSISTED` means the audit row committed to PostgreSQL. It does not mean a separate business ledger has applied account balances.

## Crash/restart behaviour

- Audit commits each journal row idempotently using `(stream_id, sequence_id)` as its primary key.
- The `stream_id` is stored in the journal header's reserved bytes at offset 32. It survives an Audit restart while the same `/dev/shm` ring exists.
- A fresh Docker IPC namespace creates a new ring and receives a new stream ID, so sequence IDs can restart at zero without colliding with old PostgreSQL rows.
- Audit publishes the ACK before advancing `journal.consumerSeq`. If it crashes between these operations, it safely replays the row and may publish a duplicate ACK. Gateway deduplicates using its pending `(connectionId, transactionId)` registry.
- If the ACK ring is full, Audit waits and does not release the journal slot. This propagates backpressure instead of dropping ACKs.

## Development database credentials

Compose uses development-only defaults:

- Database: `g12audit`
- Username: `g12`
- Password: `g12_dev_password`

Change these before using the project outside local development. Override them with `G12_DB_URL`, `G12_DB_USER`, and `G12_DB_PASSWORD`.

## Stop

```bash
docker compose down
```

This removes the containers and their temporary IPC namespace. The PostgreSQL named volume is retained. To delete development database data too:

```bash
docker compose down -v
```

Do not run multiple Gateway replicas or multiple Audit consumers against these SPSC rings. The initial implementation intentionally uses one writer/reader owner for each side of every ring.
