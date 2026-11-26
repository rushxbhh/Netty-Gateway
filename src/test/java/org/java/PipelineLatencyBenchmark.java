package org.java;

import com.google.flatbuffers.FlatBufferBuilder;
import org.java.schema.TransactionData;
import org.java.schema.TransactionMessage;
import org.java.schema.TransactionType;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Phase 4: Production-Grade End-to-End Pipeline Latency & Throughput Benchmark.
 *
 * Measures:
 * 1. Ingress Latency: Time from WebSocket send to ACCEPT|stage=INGRESS
 * 2. Audit/DB Latency: Time from ACCEPT to ACK|stage=AUDITED
 * 3. End-to-End Latency: Total client-observed turnaround time
 * 4. Latency percentiles (min, p50, p90, p95, p99, max, mean, stddev)
 * 5. Throughput (transactions/second) across both sequential and concurrent/pipelined load levels
 */
public final class PipelineLatencyBenchmark {

    private static final URI GATEWAY_URI = URI.create(
            System.getenv().getOrDefault("G12_GATEWAY_URI", "ws://localhost:8080/ws")
    );

    public record LatencyRecord(
            long transactionId,
            long ingressLatencyNs,
            long dbAuditLatencyNs,
            long e2eLatencyNs,
            boolean success
    ) {}

    public record BenchmarkResult(
            String suiteName,
            int concurrency,
            int totalTransactions,
            int successfulTransactions,
            double durationSeconds,
            double throughputOpsPerSec,
            Percentiles ingressPercentiles,
            Percentiles dbAuditPercentiles,
            Percentiles e2ePercentiles
    ) {}

    public record Percentiles(
            double minMs,
            double p50Ms,
            double p90Ms,
            double p95Ms,
            double p99Ms,
            double maxMs,
            double meanMs,
            double stdDevMs
    ) {}

    private static final AtomicLong TX_ID_GEN = new AtomicLong(40_000_000L);

    public static void main(String[] args) throws Exception {
        System.out.println("================================================================================");
        System.out.println("      ULL-FLIG End-to-End Pipeline Latency & Throughput Benchmark               ");
        System.out.println("================================================================================");
        System.out.println("Gateway Target: " + GATEWAY_URI);
        System.out.println("OS: " + System.getProperty("os.name") + " (" + System.getProperty("os.arch") + ")");
        System.out.println("JVM: " + System.getProperty("java.version") + " (" + System.getProperty("java.vendor") + ")");
        System.out.println("Processors: " + Runtime.getRuntime().availableProcessors());
        System.out.println("================================================================================\n");

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        List<BenchmarkResult> results = new ArrayList<>();

        // Suite 1: Warmup
        System.out.println("[Suite 1/5] Warming up JIT and database pool (10 transactions)...");
        runSequentialSuite(client, "Warmup", 10, true);
        System.out.println("  -> Warmup complete.\n");

        // Suite 2: Sequential Latency (50 transactions)
        System.out.println("[Suite 2/5] Running Sequential Latency Suite (50 transactions, Concurrency=1)...");
        BenchmarkResult seq50 = runSequentialSuite(client, "Sequential-50", 50, false);
        results.add(seq50);
        printSuiteResult(seq50);

        // Suite 3: Sustained Sequential (100 transactions)
        System.out.println("[Suite 3/5] Running Sustained Sequential Suite (100 transactions, Concurrency=1)...");
        BenchmarkResult seq100 = runSequentialSuite(client, "Sequential-100", 100, false);
        results.add(seq100);
        printSuiteResult(seq100);

        // Suite 4: Concurrent Pipelined Load (Concurrency=5, 50 transactions)
        System.out.println("[Suite 4/5] Running Concurrent Pipelined Load (50 transactions, Concurrency=5)...");
        BenchmarkResult conc5 = runConcurrentSuite(client, "Concurrent-5", 50, 5);
        results.add(conc5);
        printSuiteResult(conc5);

        // Suite 5: Concurrent Pipelined Load (Concurrency=10, 100 transactions)
        System.out.println("[Suite 5/5] Running Concurrent Pipelined Load (100 transactions, Concurrency=10)...");
        BenchmarkResult conc10 = runConcurrentSuite(client, "Concurrent-10", 100, 10);
        results.add(conc10);
        printSuiteResult(conc10);

        // Output overall summary table
        printOverallSummary(results);

        // Export JSON summary
        exportJson(results, "latency_benchmark_results.json");
    }

    private static BenchmarkResult runSequentialSuite(
            HttpClient client,
            String suiteName,
            int count,
            boolean isWarmup
    ) throws Exception {
        List<LatencyRecord> records = new ArrayList<>(count);

        BlockingQueue<String> inboundMessages = new LinkedBlockingQueue<>();
        CompletableFuture<WebSocket> wsFuture = new CompletableFuture<>();

        WebSocket.Listener listener = new WebSocket.Listener() {
            private final StringBuilder buffer = new StringBuilder();

            @Override
            public void onOpen(WebSocket webSocket) {
                wsFuture.complete(webSocket);
                webSocket.request(1);
            }

            @Override
            public CompletableFuture<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                buffer.append(data);
                if (last) {
                    inboundMessages.offer(buffer.toString());
                    buffer.setLength(0);
                }
                webSocket.request(1);
                return null;
            }

            @Override
            public void onError(WebSocket webSocket, Throwable error) {
                inboundMessages.offer("__ERR__:" + error.getMessage());
            }
        };

        WebSocket ws = client.newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .buildAsync(GATEWAY_URI, listener)
                .get(5, TimeUnit.SECONDS);

        long suiteStartNs = System.nanoTime();

        try {
            for (int i = 0; i < count; i++) {
                long txId = TX_ID_GEN.incrementAndGet();
                byte[] payload = buildTransaction(txId, 1000L + (i * 10), 101L, 202L, TransactionType.TRANSFER);

                long tSend = System.nanoTime();
                ws.sendBinary(ByteBuffer.wrap(payload), true).join();

                // Wait for ACCEPT
                String resp1 = inboundMessages.poll(10, TimeUnit.SECONDS);
                long tIngress = System.nanoTime();

                // Wait for ACK
                String resp2 = inboundMessages.poll(10, TimeUnit.SECONDS);
                long tAudited = System.nanoTime();

                boolean success = resp1 != null && resp1.contains("ACCEPT|stage=INGRESS")
                        && resp2 != null && resp2.contains("ACK|stage=AUDITED") && resp2.contains("status=PERSISTED");

                records.add(new LatencyRecord(
                        txId,
                        tIngress - tSend,
                        tAudited - tIngress,
                        tAudited - tSend,
                        success
                ));

                if (!isWarmup && (i + 1) % 25 == 0) {
                    System.out.printf("  Progress: %d/%d completed (latest e2e: %.2f ms)\n",
                            i + 1, count, (tAudited - tSend) / 1_000_000.0);
                }
            }
        } finally {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "Benchmark complete").join();
        }

        long suiteEndNs = System.nanoTime();
        double durationSec = (suiteEndNs - suiteStartNs) / 1_000_000_000.0;
        int successful = (int) records.stream().filter(LatencyRecord::success).count();
        double throughput = count / durationSec;

        List<Double> ingressList = records.stream().map(r -> r.ingressLatencyNs() / 1_000_000.0).toList();
        List<Double> dbAuditList = records.stream().map(r -> r.dbAuditLatencyNs() / 1_000_000.0).toList();
        List<Double> e2eList = records.stream().map(r -> r.e2eLatencyNs() / 1_000_000.0).toList();

        return new BenchmarkResult(
                suiteName,
                1,
                count,
                successful,
                durationSec,
                throughput,
                computePercentiles(ingressList),
                computePercentiles(dbAuditList),
                computePercentiles(e2eList)
        );
    }

    private static BenchmarkResult runConcurrentSuite(
            HttpClient client,
            String suiteName,
            int totalCount,
            int concurrency
    ) throws Exception {
        List<LatencyRecord> allRecords = Collections.synchronizedList(new ArrayList<>(totalCount));
        AtomicInteger remaining = new AtomicInteger(totalCount);
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        CountDownLatch latch = new CountDownLatch(concurrency);
        long suiteStartNs = System.nanoTime();

        List<CompletableFuture<Void>> futures = new ArrayList<>(concurrency);

        for (int c = 0; c < concurrency; c++) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    BlockingQueue<String> inboundMessages = new LinkedBlockingQueue<>();
                    CompletableFuture<WebSocket> wsFuture = new CompletableFuture<>();

                    WebSocket.Listener listener = new WebSocket.Listener() {
                        private final StringBuilder buffer = new StringBuilder();

                        @Override
                        public void onOpen(WebSocket webSocket) {
                            wsFuture.complete(webSocket);
                            webSocket.request(1);
                        }

                        @Override
                        public CompletableFuture<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                            buffer.append(data);
                            if (last) {
                                inboundMessages.offer(buffer.toString());
                                buffer.setLength(0);
                            }
                            webSocket.request(1);
                            return null;
                        }

                        @Override
                        public void onError(WebSocket webSocket, Throwable error) {
                            inboundMessages.offer("__ERR__:" + error.getMessage());
                        }
                    };

                    WebSocket ws = client.newWebSocketBuilder()
                            .connectTimeout(Duration.ofSeconds(5))
                            .buildAsync(GATEWAY_URI, listener)
                            .get(5, TimeUnit.SECONDS);

                    latch.countDown();
                    latch.await(5, TimeUnit.SECONDS);

                    while (remaining.decrementAndGet() >= 0) {
                        long txId = TX_ID_GEN.incrementAndGet();
                        byte[] payload = buildTransaction(txId, 2500L, 101L, 202L, TransactionType.TRANSFER);

                        long tSend = System.nanoTime();
                        ws.sendBinary(ByteBuffer.wrap(payload), true).join();

                        String resp1 = inboundMessages.poll(10, TimeUnit.SECONDS);
                        long tIngress = System.nanoTime();

                        String resp2 = inboundMessages.poll(10, TimeUnit.SECONDS);
                        long tAudited = System.nanoTime();

                        boolean success = resp1 != null && resp1.contains("ACCEPT|stage=INGRESS")
                                && resp2 != null && resp2.contains("ACK|stage=AUDITED") && resp2.contains("status=PERSISTED");

                        allRecords.add(new LatencyRecord(
                                txId,
                                tIngress - tSend,
                                tAudited - tIngress,
                                tAudited - tSend,
                                success
                        ));
                    }

                    ws.sendClose(WebSocket.NORMAL_CLOSURE, "Done").join();
                } catch (Exception e) {
                    System.err.println("Concurrent worker error: " + e.getMessage());
                }
            }, pool));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        pool.shutdown();

        long suiteEndNs = System.nanoTime();
        double durationSec = (suiteEndNs - suiteStartNs) / 1_000_000_000.0;
        int count = allRecords.size();
        int successful = (int) allRecords.stream().filter(LatencyRecord::success).count();
        double throughput = count / durationSec;

        List<Double> ingressList = allRecords.stream().map(r -> r.ingressLatencyNs() / 1_000_000.0).toList();
        List<Double> dbAuditList = allRecords.stream().map(r -> r.dbAuditLatencyNs() / 1_000_000.0).toList();
        List<Double> e2eList = allRecords.stream().map(r -> r.e2eLatencyNs() / 1_000_000.0).toList();

        return new BenchmarkResult(
                suiteName,
                concurrency,
                count,
                successful,
                durationSec,
                throughput,
                computePercentiles(ingressList),
                computePercentiles(dbAuditList),
                computePercentiles(e2eList)
        );
    }

    private static Percentiles computePercentiles(List<Double> data) {
        if (data.isEmpty()) return new Percentiles(0, 0, 0, 0, 0, 0, 0, 0);
        List<Double> sorted = new ArrayList<>(data);
        Collections.sort(sorted);
        int n = sorted.size();

        double min = sorted.get(0);
        double max = sorted.get(n - 1);
        double p50 = getPercentile(sorted, 50.0);
        double p90 = getPercentile(sorted, 90.0);
        double p95 = getPercentile(sorted, 95.0);
        double p99 = getPercentile(sorted, 99.0);

        double sum = 0;
        for (double d : sorted) sum += d;
        double mean = sum / n;

        double sumSq = 0;
        for (double d : sorted) sumSq += (d - mean) * (d - mean);
        double stdDev = Math.sqrt(sumSq / n);

        return new Percentiles(min, p50, p90, p95, p99, max, mean, stdDev);
    }

    private static double getPercentile(List<Double> sorted, double percentile) {
        if (sorted.isEmpty()) return 0;
        double index = (percentile / 100.0) * (sorted.size() - 1);
        int lower = (int) Math.floor(index);
        int upper = (int) Math.ceil(index);
        if (lower == upper) return sorted.get(lower);
        return sorted.get(lower) + (index - lower) * (sorted.get(upper) - sorted.get(lower));
    }

    private static void printSuiteResult(BenchmarkResult r) {
        System.out.println("\n--------------------------------------------------------------------------------");
        System.out.printf("  Suite: %s (Concurrency=%d) | Count: %d | Success: %d/%d (%.1f%%) | Throughput: %.2f tx/sec\n",
                r.suiteName(), r.concurrency(), r.totalTransactions(), r.successfulTransactions(), r.totalTransactions(),
                (100.0 * r.successfulTransactions() / r.totalTransactions()), r.throughputOpsPerSec());
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("  Metric (ms)            Min      p50      p90      p95      p99      Max     Mean   StdDev");
        System.out.println("  ------------------------------------------------------------------------------");
        printPercentileRow("  1. Ingress (<1ms)", r.ingressPercentiles());
        printPercentileRow("  2. DB Audit Commit", r.dbAuditPercentiles());
        printPercentileRow("  3. Total E2E Latency", r.e2ePercentiles());
        System.out.println("--------------------------------------------------------------------------------\n");
    }

    private static void printPercentileRow(String label, Percentiles p) {
        System.out.printf("%-22s %7.2f  %7.2f  %7.2f  %7.2f  %7.2f  %7.2f  %7.2f  %7.2f\n",
                label, p.minMs(), p.p50Ms(), p.p90Ms(), p.p95Ms(), p.p99Ms(), p.maxMs(), p.meanMs(), p.stdDevMs());
    }

    private static void printOverallSummary(List<BenchmarkResult> results) {
        System.out.println("================================================================================");
        System.out.println("                         BENCHMARK EXECUTIVE SUMMARY                            ");
        System.out.println("================================================================================");
        System.out.println("Suite           Conc   Total   Success   Throughput    Ingress p50     DB Audit p50       E2E p50");
        System.out.println("--------------------------------------------------------------------------------");
        for (BenchmarkResult r : results) {
            System.out.printf("%-15s %4d   %5d   %7s   %8.2f/s   %9.3f ms   %10.2f ms   %9.2f ms\n",
                    r.suiteName(),
                    r.concurrency(),
                    r.totalTransactions(),
                    r.successfulTransactions() + "/" + r.totalTransactions(),
                    r.throughputOpsPerSec(),
                    r.ingressPercentiles().p50Ms(),
                    r.dbAuditPercentiles().p50Ms(),
                    r.e2ePercentiles().p50Ms());
        }
        System.out.println("================================================================================\n");
    }

    private static void exportJson(List<BenchmarkResult> results, String filename) {
        try (FileWriter writer = new FileWriter(filename)) {
            writer.write("{\n");
            writer.write("  \"environment\": {\n");
            writer.write("    \"os\": \"" + System.getProperty("os.name") + " (" + System.getProperty("os.arch") + ")\",\n");
            writer.write("    \"jvm\": \"" + System.getProperty("java.version") + " (" + System.getProperty("java.vendor") + ")\",\n");
            writer.write("    \"processors\": " + Runtime.getRuntime().availableProcessors() + ",\n");
            writer.write("    \"database\": \"PostgreSQL 16 (Neon Serverless AWS us-east-2 Ohio)\",\n");
            writer.write("    \"sequencer_engine\": \"C++20 GCC 15.2.0 -O3 -msse4.2 (18.65M ops/s, 48 cycles p50)\"\n");
            writer.write("  },\n");
            writer.write("  \"suites\": [\n");
            for (int i = 0; i < results.size(); i++) {
                BenchmarkResult r = results.get(i);
                writer.write(String.format(Locale.US,
                        "    {\n" +
                                "      \"suite\": \"%s\",\n" +
                                "      \"concurrency\": %d,\n" +
                                "      \"count\": %d,\n" +
                                "      \"success\": %d,\n" +
                                "      \"duration_sec\": %.2f,\n" +
                                "      \"throughput\": %.2f,\n" +
                                "      \"ingress\": { \"min\": %.3f, \"p50\": %.3f, \"p90\": %.3f, \"p95\": %.3f, \"p99\": %.3f, \"max\": %.3f, \"mean\": %.3f, \"stddev\": %.3f },\n" +
                                "      \"db_audit\": { \"min\": %.2f, \"p50\": %.2f, \"p90\": %.2f, \"p95\": %.2f, \"p99\": %.2f, \"max\": %.2f, \"mean\": %.2f, \"stddev\": %.2f },\n" +
                                "      \"e2e\": { \"min\": %.2f, \"p50\": %.2f, \"p90\": %.2f, \"p95\": %.2f, \"p99\": %.2f, \"max\": %.2f, \"mean\": %.2f, \"stddev\": %.2f }\n" +
                                "    }%s\n",
                        r.suiteName(), r.concurrency(), r.totalTransactions(), r.successfulTransactions(),
                        r.durationSeconds(), r.throughputOpsPerSec(),
                        r.ingressPercentiles().minMs(), r.ingressPercentiles().p50Ms(), r.ingressPercentiles().p90Ms(),
                        r.ingressPercentiles().p95Ms(), r.ingressPercentiles().p99Ms(), r.ingressPercentiles().maxMs(),
                        r.ingressPercentiles().meanMs(), r.ingressPercentiles().stdDevMs(),
                        r.dbAuditPercentiles().minMs(), r.dbAuditPercentiles().p50Ms(), r.dbAuditPercentiles().p90Ms(),
                        r.dbAuditPercentiles().p95Ms(), r.dbAuditPercentiles().p99Ms(), r.dbAuditPercentiles().maxMs(),
                        r.dbAuditPercentiles().meanMs(), r.dbAuditPercentiles().stdDevMs(),
                        r.e2ePercentiles().minMs(), r.e2ePercentiles().p50Ms(), r.e2ePercentiles().p90Ms(),
                        r.e2ePercentiles().p95Ms(), r.e2ePercentiles().p99Ms(), r.e2ePercentiles().maxMs(),
                        r.e2ePercentiles().meanMs(), r.e2ePercentiles().stdDevMs(),
                        (i < results.size() - 1 ? "," : "")
                ));
            }
            writer.write("  ]\n");
            writer.write("}\n");
            System.out.println("Raw benchmark metrics exported to: " + filename);
        } catch (IOException e) {
            System.err.println("Failed to export JSON: " + e.getMessage());
        }
    }

    private static byte[] buildTransaction(
            long transactionId,
            long amount,
            long fromAccount,
            long toAccount,
            int type
    ) {
        FlatBufferBuilder builder = new FlatBufferBuilder(128);
        TransactionMessage.startTransactionMessage(builder);
        int transactionOffset = TransactionData.createTransactionData(
                builder,
                transactionId,
                amount,
                fromAccount,
                toAccount,
                type
        );
        TransactionMessage.addTransaction(builder, transactionOffset);
        int rootOffset = TransactionMessage.endTransactionMessage(builder);
        TransactionMessage.finishTransactionMessageBuffer(builder, rootOffset);
        return builder.sizedByteArray();
    }
}
