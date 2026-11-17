# G12 Ingress Gateway + Sequencer + Audit

A three-service Java/C++ application using Linux shared-memory SPSC rings.

## Services

- **Gateway** — Java 21 + Netty WebSocket ingress; validates FlatBuffers and publishes to the inbound ring.
- **Sequencer** — C++20; consumes inbound records, assigns a sequence, and publishes 64-byte journal records.
- **Audit** — Java 21; validates journal order and CRC32C, persists records to PostgreSQL, and publishes an ACK.
- **PostgreSQL** — durable audit storage.

## End-to-end message flow

```text
WebSocket client
   -> Gateway: ACCEPT|stage=INGRESS
   -> inbound shared-memory ring
   -> C++ Sequencer
   -> journal shared-memory ring
   -> Java Audit
   -> PostgreSQL commit
   -> ACK shared-memory ring
   -> Gateway
   -> original WebSocket client: ACK|stage=AUDITED
```

The Gateway correlates the final acknowledgement using `(connectionId, transactionId)`. `PERSISTED` means the journal audit row committed to PostgreSQL; it does not mean a separate account ledger has applied the transaction.

## Run the complete integration test

Requires Docker Desktop or Docker Engine with Compose v2 and Linux containers enabled.

```bash
chmod +x scripts/test-three-services.sh
./scripts/test-three-services.sh
```

The script builds all containers, starts the Sequencer, PostgreSQL, Audit and Gateway, verifies the shared IPC namespace, sends WebSocket test transactions, checks ring counters, and confirms the expected PostgreSQL row.

Expected successful WebSocket responses for a valid test transaction:

```text
ACCEPT|stage=INGRESS|transactionId=1001|inboundSequence=...
ACK|stage=AUDITED|transactionId=1001|journalSequence=...|status=PERSISTED
```

## Inspect and stop

```bash
docker compose ps
docker compose logs -f sequencer audit gateway
docker compose down
```

`docker compose down` keeps the named PostgreSQL volume. To delete the development database too, use `docker compose down -v`.

## Local protocol checks (Java 21 + CMake + C++ compiler)

```bash
./scripts/test-audit-protocol.sh
cmake -S sequencer -B /tmp/g12-sequencer-build -DCMAKE_BUILD_TYPE=Release
cmake --build /tmp/g12-sequencer-build --target g12_sequencer test_sequencer -j2
/tmp/g12-sequencer-build/test_sequencer
```

## Protocol documentation

See [`RING_PROTOCOL.md`](RING_PROTOCOL.md), [`MAPPED_RING_WRITER.md`](MAPPED_RING_WRITER.md), [`DOCKER_LINUX_SETUP.md`](DOCKER_LINUX_SETUP.md), and [`audit-service/README.md`](audit-service/README.md).

The shared-memory protocol is intended for little-endian Linux and one producer/one consumer per ring. Development PostgreSQL credentials are defined in `docker-compose.yml`; replace them before using outside local development.
