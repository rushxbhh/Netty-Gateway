# ULL-FLIG — External Examiner Demonstration Runbook

This runbook provides a repeatable, end-to-end live demonstration of the **ULL-FLIG** (*Ultra-Low-Latency Financial Ledger & Ingress Gateway*) pipeline.

---

## 1. Prerequisites & Environment

- **OS:** Windows 11 (or Linux with `/dev/shm`)
- **Java:** JDK 21 (`C:\Program Files\Java\jdk-21`)
- **C++ Compiler:** GCC 15.2+ (`g++ -std=c++20 -O3 -msse4.2`)
- **Database:** PostgreSQL 16 (configured via `.env` with `G12_DB_URL`, `G12_DB_USER`, `G12_DB_PASSWORD`)

---

## 2. Step-by-Step Demonstration Script

### Step 1 — Start the 3-Service Cluster
From the repository root:
```powershell
.\scripts\start-native.ps1
```
Expected output:
- `[1/3] Launching C++ Sequencer Daemon...`
- `[2/3] Launching Java Audit Service...`
- `[3/3] Launching Netty WebSocket Gateway...`

### Step 2 — Verify Health & Service Status
1. Open `http://localhost:8080/` in a browser.
2. Confirm all four topbar indicators are green:
   - **Sequencer:** `ONLINE`
   - **Audit:** `ONLINE`
   - **PostgreSQL:** `CONNECTED`
   - **WebSocket:** `CONNECTED`
3. Optionally query the JSON health endpoint:
   ```powershell
   Invoke-RestMethod http://localhost:8080/health
   ```
   Returns HTTP `200` with `{"status":"UP","services":{...}}`.

### Step 3 — Submit a Valid Transfer & Trace All 5 Stages
1. In the **Transaction Ingress** panel, select **TRANSFER** (or click **Transfer $25**).
2. Click **Send Binary Transaction**.
3. Observe the **Pipeline Execution Trace** and **WebSocket Frame Stream**:
   - **Stage 1 (`Client Frame`):** 60-byte FlatBuffer (`NIGT` identifier) sent over binary WebSocket.
   - **Stage 2 (`Netty Ingress`):** `ACCEPT|stage=INGRESS|transactionId=...|inboundSequence=...` emitted immediately. Explain that this confirms ring ingress *only*, not durable storage.
   - **Stage 3 (`C++ Sequencer`):** Assigns monotonic `sequenceId`, captures `RDTSC` cycle timestamp, computes hardware SSE4.2 `CRC32C`, writes 64-byte slot to `g12-journal.ring`.
   - **Stage 4 (`Audit Commit`):** Java Audit Service verifies `CRC32C` and executes idempotent PostgreSQL commit (`ON CONFLICT (stream_id, sequence_id) DO UPDATE`, batched up to 32 records).
   - **Stage 5 (`ACK Return`):** `ACK|stage=AUDITED|transactionId=...|journalSequence=...|status=PERSISTED` delivered via `g12-ack.ring` back to the originating WebSocket connection.

### Step 4 — Inspect the Durable PostgreSQL Audit Record
1. Look at the **Durable Audit Ledger** table at the bottom of the **Live Gateway** tab.
2. Verify the new row displays:
   - Monotonic `Seq` ID matching `journalSequence`
   - Matching `Txn ID`, `Type` (`TRANSFER`), `From`, `To`, `Amount` (`$25.00`)
   - `PERSISTED` status badge, hex `CRC32C` checksum, and `Audited At` timestamp.
3. Point out the **Inbound**, **Journal**, and **ACK** ring counters incremented equally with `Depth = 0`.

### Step 5 — Submit an Invalid Transaction & Verify Rejection
1. Click **Zero Amount (Reject)** in **Test Scenarios** (sets `Amount = 0`).
2. Click **Send Binary Transaction**.
3. Observe:
   - Gateway immediately returns `REJECT|code=INVALID_AMOUNT|message=...` in the **WebSocket Frame Stream**.
   - Stage 2 highlights in red (`REJECTED`).
   - **Ring counters and PostgreSQL ledger do not advance** — invalid payloads never enter the shared-memory rings.

### Step 6 — Explain Architecture & Performance Benchmarks
1. Switch to the **Architecture** tab:
   - Walk through the animated SPSC pipeline diagram, highlighting the 3 lock-free rings (`128 B` Inbound, `64 B` Journal, `64 B` ACK) and the two-stage ACK contract.
2. Switch to the **Performance** tab:
   - Show the live **Chart.js** charts and percentile table from `latency_benchmark_results.json`:
     - C++ Sequencer: **21–24.8 M ops/s**, **~47–51 CPU cycles** p50.
     - Gateway Ingress: **0.71–1.21 ms** p50 across all suites.
     - JDBC Batch Commit + Singapore PostgreSQL: **185–199 ms** p50, **50.66 tx/s** at `Concurrency=10` (**100% success across 300/300 transactions**).

### Step 7 — Clean Shutdown
```powershell
.\scripts\stop-native.ps1
```
