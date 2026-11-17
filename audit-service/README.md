# G12 Java Audit Service

A small Java 21 process that consumes the existing C++ 64-byte journal ring,
verifies sequence order and CRC32C, persists each record idempotently to
PostgreSQL, publishes a 64-byte ACK record, and only then releases the journal
slot by advancing `journal.consumerSeq`.

## Run through Docker Compose

From the repository root:

```bash
./scripts/test-three-services.sh
```

The Compose stack contains:

- `sequencer`: existing C++20 sequencer; owns the shared IPC namespace.
- `postgres`: durable audit database with a named volume.
- `audit`: this Java service; consumes journal and produces ACK records.
- `gateway`: existing Netty ingress extended with an ACK-ring reader.

## Journal consumer ordering

For each journal slot, Audit executes:

1. Acquire-load `journal.producerSeq` and read the next published 64-byte slot.
2. Verify that `sequenceId == journal.consumerSeq` and CRC32C over bytes 0..59.
3. Insert into PostgreSQL in a transaction. Replays of the same sequence are
   accepted only when every immutable field matches the existing row.
4. Publish the ACK slot with release semantics. If the ACK ring is full, wait.
5. Release-store `journal.consumerSeq + 1`.

This order ensures a record is not released before it is durable and its ACK is
available to the Gateway. A crash after database commit but before ring-counter
advancement replays the record; the idempotent database insert makes that safe.

## ACK ring ABI

The Audit Service assigns a non-zero `stream_id` in the journal ring header's
reserved bytes (offset 32). It is written once under the startup file lock and
survives an Audit restart while the same shared-memory ring exists. A fresh
Docker IPC namespace starts with a fresh ring and therefore gets a fresh stream
ID. PostgreSQL keys records by `(stream_id, sequence_id)`, so a new volatile ring
starting again at sequence zero does not conflict with rows from an earlier
Docker run.

The ACK ring uses the same 192-byte control/header prefix as the other rings:

| Slot offset | Bytes | Field |
|---:|---:|---|
| 0 | 8 | Journal sequence |
| 8 | 8 | Client transaction ID |
| 16 | 8 | Gateway connection ID |
| 24 | 8 | Audit time (Unix epoch milliseconds) |
| 32 | 4 | Audit status (`1 = PERSISTED`) |
| 36 | 4 | Original journal status (`0 = OK`, `1 = malformed`) |
| 40 | 20 | Reserved, zero |
| 60 | 4 | CRC32C over bytes 0..59 |

Ring kind is `3`; slot size is 64 bytes; capacity is 65,536. File size is
4,194,496 bytes. Audit owns ACK `producerSeq`; Gateway owns ACK `consumerSeq`.

## Current acknowledgement meaning

The Gateway first returns `ACCEPT|stage=INGRESS` after publishing to the inbound
ring. Later, after PostgreSQL commit and ACK-ring publication, it sends
`ACK|stage=AUDITED|transactionId=...|journalSequence=...|status=PERSISTED` over
the originating WebSocket channel.

`PERSISTED` means the audit journal row is committed to PostgreSQL. It does not
mean a separate business ledger has applied a debit/credit operation.

## Local development database

Compose defaults are intentionally development-only:

- database: `g12audit`
- username: `g12`
- password: `g12_dev_password`

Override `G12_DB_URL`, `G12_DB_USER`, and `G12_DB_PASSWORD` outside development.
