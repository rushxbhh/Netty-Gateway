# ULL-FLIG — Ultra-Low-Latency Financial Ledger & Ingress Gateway

A B.Tech final-year project demonstrating a production-grade, three-service transaction ingress and audit pipeline built with **Java 21 + Netty**, **C++20**, and **PostgreSQL**, communicating via lock-free SPSC shared-memory ring buffers.

---

## Architecture Overview

```text
WebSocket Client
  → Gateway (Java/Netty)        — validates FlatBuffers, publishes to inbound ring
      → Inbound Ring (mmap)
          → Sequencer (C++20)   — assigns monotonic sequence, writes 64-byte journal record
              → Journal Ring (mmap)
                  → Audit (Java) — validates CRC32C + order, persists to PostgreSQL, publishes ACK
                      → ACK Ring (mmap)
                          → Gateway               — correlates ACK to original WebSocket connection
                              → WebSocket Client  — receives PERSISTED confirmation
```

**Two-stage acknowledgement:**
- `ACCEPT|stage=INGRESS` — Gateway accepted the message (not yet persisted)
- `ACK|stage=AUDITED|status=PERSISTED` — PostgreSQL commit confirmed

See [`PROJECT_STATUS.md`](PROJECT_STATUS.md) for full architectural detail and [`RING_PROTOCOL.md`](RING_PROTOCOL.md) for the binary wire format.

---

## Services

| Service | Language | Role |
|---|---|---|
| **Gateway** | Java 21 + Netty | WebSocket ingress, FlatBuffers validation, ring I/O |
| **Sequencer** | C++20 (GCC -O3 -msse4.2) | SPSC ring consumer/producer, monotonic sequencing |
| **Audit** | Java 21 | CRC32C validation, PostgreSQL persistence, ACK publish |
| **PostgreSQL** | 16 | Durable audit ledger storage |

---

## Prerequisites

### Native (Windows — primary development environment)

| Requirement | Version | Notes |
|---|---|---|
| JDK | 21 | `C:\Program Files\Java\jdk-21` or any JDK 21 on `PATH` |
| Apache Maven | 3.9+ | Used via `.mvn/wrapper/mvn.cmd` (wrapper bundled) |
| GCC / G++ | 15.2+ (C++20) | Via MSYS2 UCRT64: `C:\msys64\ucrt64\bin\g++.exe` |
| PostgreSQL | 16 (cloud or local) | Neon Serverless recommended for zero-setup |

Install MSYS2 from https://www.msys2.org/, then in MSYS2 terminal:
```bash
pacman -S mingw-w64-ucrt-x86_64-gcc
```

### Docker (Mac / Linux — teammate environments)

| Requirement | Notes |
|---|---|
| Docker Desktop (Mac/Linux) | Compose v2 included |
| Linux containers | Required; enabled by default on Mac/Linux |

---

## Environment Setup

Copy the example environment file and fill in your database credentials:

```powershell
copy .env.example .env
```

Edit `.env`:
```env
G12_DB_URL=jdbc:postgresql://<host>/<dbname>?sslmode=require
G12_DB_USER=<username>
G12_DB_PASSWORD=<password>
```

> **Never commit `.env`.** It is listed in `.gitignore`. Use Neon (https://neon.tech) for a free PostgreSQL 16 cloud instance.

For Docker, the same variables are read from `.env` automatically by `docker-compose.yml`.

---

## Running Natively (Windows)

### First-time / fresh checkout

```powershell
.\scripts\start-native.ps1 -BuildFirst
```

This compiles the C++ sequencer, runs `mvn package` for both JARs, copies runtime dependencies, and starts all three services.

### Subsequent starts (JARs already built)

```powershell
.\scripts\start-native.ps1
```

### Stop all services

```powershell
.\scripts\stop-native.ps1
```

### Expected startup output

```
==========================================================
  Starting ULL-FLIG Pipeline Services Natively (Windows)
==========================================================
[Config] Loading environment variables from .env
[Build] Compiling C++ Sequencer...
[Build] C++ Sequencer built successfully.
[1/3] Launching C++ Sequencer Daemon...
[2/3] Launching Java Audit Service...
[3/3] Launching Netty WebSocket Gateway...
[OK] All services started. Gateway: http://localhost:8080/
```

---

## Running with Docker (Mac / Linux)

### Start all services

```bash
docker compose up -d --build
```

### View logs

```bash
docker compose logs -f sequencer audit gateway
```

### Stop (keep database volume)

```bash
docker compose down
```

### Stop and delete database

```bash
docker compose down -v
```

---

## Frontend Dashboard

After starting services, open **http://localhost:8080/** in a browser.

**Tabs:**
- **Dashboard** — Submit transactions, view the 5-stage pipeline stepper, live ACK stream
- **Architecture** — Animated SVG diagram showing data flow through all three services and rings
- **Performance** — Latency and throughput charts from the benchmark suite

**Topbar indicators** (live, polled every 2 s):
- 🟢 Sequencer / Audit / PostgreSQL / WebSocket — green = ONLINE/CONNECTED

**Submitting a transaction:**
1. Enter a Transfer description and an amount > 0.
2. Click **Submit**.
3. Watch the stepper advance: Ingress → Sequencer → Journal → Audit → Persisted.
4. The final ACK reads `PERSISTED` when the PostgreSQL commit is confirmed.

**Validation test:** Submit amount = `0` → Gateway rejects with `REJECTED|reason=ZERO_AMOUNT`.

---

## Running Tests

### C++ Sequencer Unit Tests

```powershell
# From repo root (Windows — adjust path if g++ is on PATH)
$gpp = "C:\msys64\ucrt64\bin\g++.exe"
& $gpp -std=c++20 -O3 -msse4.2 -I sequencer/include `
    sequencer/tests/test_sequencer.cpp `
    sequencer/src/crc32c.cpp `
    -o sequencer/test_sequencer.exe
.\sequencer\test_sequencer.exe
```

Expected: **8/8 PASS**, sequencer throughput ~18–21 M ops/s.

### Java Protocol Self-Tests (no cluster required)

```powershell
$mvn = ".mvn\wrapper\mvn.cmd"
# Audit protocol encoding/decoding
& $mvn -f audit-service/pom.xml test -Dtest=AuditProtocolSelfTest -pl audit-service
# Gateway ↔ Audit ACK interoperability
& $mvn test -Dtest=AuditAckInteropSelfTest
```

Expected: Both **PASS** in < 2 s.

### End-to-End Reliability Suite (requires running cluster)

Start services first, then:

```powershell
$cp = "target\Netty-Ingress-Gateway-1.0-SNAPSHOT.jar;target\dependency\*"
java -cp $cp org.java.EndToEndReliabilityTestSuite
```

Expected: **12/12 scenarios PASS** — valid transfers, zero-amount rejection, duplicate handling, concurrent submissions, batch ordering.

### Pipeline Latency Benchmark (requires running cluster)

```powershell
$cp = "target\Netty-Ingress-Gateway-1.0-SNAPSHOT.jar;target\dependency\*"
java -cp $cp org.java.PipelineLatencyBenchmark
```

Results are saved to `latency_benchmark_results.json`.

### Docker Integration Test (Mac / Linux)

```bash
chmod +x scripts/test-three-services.sh
./scripts/test-three-services.sh
```

Builds all containers, starts the cluster, sends WebSocket transactions, checks ring counters, and verifies the PostgreSQL audit row.

---

## Benchmark Results

Measured on Windows 11 (12-core), Neon PostgreSQL 16 (AWS us-east-2), JVM 21.0.4.

### C++ Sequencer (in-process, no I/O)

| Metric | Result |
|---|---|
| Throughput | **18.65 M ops/s** |
| Latency p50 | **48 cycles** |

### End-to-End Pipeline (WebSocket → PostgreSQL commit → ACK)

| Suite | Concurrency | Transactions | E2E p50 | Ingress p50 | DB-Audit p50 | Throughput |
|---|---|---|---|---|---|---|
| Sequential-50 | 1 | 50 | 198.47 ms | 1.21 ms | 197.01 ms | 5.00 tx/s |
| Sequential-100 | 1 | 100 | 187.29 ms | 0.89 ms | 186.42 ms | 5.19 tx/s |
| Concurrent-5 | 5 | 50 | 185.80 ms | 0.71 ms | 185.08 ms | **24.41 tx/s** |
| Concurrent-10 | 10 | 100 | 200.03 ms | 0.74 ms | 199.15 ms | **50.66 tx/s** |

**Key observations:**
- Ingress latency (Gateway → ring publish) is sub-millisecond in all scenarios (~0.7–1.2 ms p50).
- PostgreSQL dominates E2E latency (~185–200 ms p50) — the network round-trip to a remote Neon instance in a different region accounts for most of this.
- Throughput scales linearly from 24.41 tx/s at concurrency=5 to 50.66 tx/s at concurrency=10, demonstrating the lock-free SPSC pipeline has no internal serialization bottleneck.
- Full percentile data: [`latency_benchmark_results.json`](latency_benchmark_results.json)
- Charts: [`benchmark-charts/`](benchmark-charts/)

---

## Health & Observability

```powershell
# JSON health check
Invoke-RestMethod http://localhost:8080/health
# → {"status":"UP","services":{"gateway":"ONLINE","sequencer":"ONLINE","audit":"ONLINE","database":"CONNECTED"}}

# Full telemetry (ring counters, latency stats, service liveness)
Invoke-RestMethod http://localhost:8080/api/telemetry
```

Degraded services return HTTP `503` with `"status":"DEGRADED"`.

---

## Project Structure

```
Netty-Gateway/
├── src/                        # Gateway (Java/Netty) source
│   └── main/resources/static/  # Frontend (index.html, styles.css, app.js)
├── audit-service/              # Audit service (Java)
├── sequencer/                  # C++ sequencer
│   ├── src/                    # main.cpp, crc32c.cpp
│   ├── include/                # ring headers
│   └── tests/                  # test_sequencer.cpp
├── scripts/                    # start-native.ps1, stop-native.ps1, test-*.sh
├── references/                 # Technical reference docs and runbooks
│   ├── demo_runbook.md         # Examiner walkthrough
│   ├── test_results.md         # Full test matrix with actual results
│   └── examiner_qa.md          # Prepared answers to examiner questions
├── benchmark-charts/           # PNG charts from benchmark suite
├── latency_benchmark_results.json
├── PROJECT_STATUS.md           # Version history and architectural detail
├── RING_PROTOCOL.md            # Binary wire format specification
└── docker-compose.yml
```

---

## Reference Documents

| Document | Purpose |
|---|---|
| [`PROJECT_STATUS.md`](PROJECT_STATUS.md) | Version history, architecture detail, design decisions |
| [`RING_PROTOCOL.md`](RING_PROTOCOL.md) | 64-byte journal record binary format, CRC32C layout |
| [`MAPPED_RING_WRITER.md`](MAPPED_RING_WRITER.md) | Shared-memory ring writer design |
| [`DOCKER_LINUX_SETUP.md`](DOCKER_LINUX_SETUP.md) | Docker + Linux container setup guide |
| [`references/demo_runbook.md`](references/demo_runbook.md) | Step-by-step examiner demonstration script |
| [`references/test_results.md`](references/test_results.md) | Full test matrix with actual pass/fail results |
| [`references/examiner_qa.md`](references/examiner_qa.md) | Prepared answers to likely examiner questions |
| [`audit-service/README.md`](audit-service/README.md) | Audit service build and configuration |

---

## License

Academic project — B.Tech Final Year, 2025–2026. Not for production use.
