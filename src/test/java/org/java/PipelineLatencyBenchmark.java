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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Phase 4: Production-Grade End-to-End Pipeline Latency & Throughput Benchmark.
 *
 * Measures:
 * 1. Ingress Latency: Time from WebSocket send to ACCEPT|stage=INGRESS (Netty framing, FlatBuffer validation, SPSC ring write)
 * 2. Audit/DB Latency: Time from ACCEPT to ACK|stage=AUDITED (C++ sequencing, journal ring, Neon PostgreSQL commit, ACK ring)
 * 3. End-to-End Latency: Total client-observed turnaround time
 * 4. Latency percentiles (min, p50, p90, p95, p99, max, mean, stddev)
 * 5. Throughput (transactions/second) across different load profiles
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

    private static final AtomicLong TX_ID_GEN = new AtomicLong(20_000_000L);

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
        System.out.println("[Suite 1/3] Warming up JIT and database pool (10 transactions)...");
        runSequentialSuite(client, "Warmup", 10, true);
        System.out.println("  -> Warmup complete.\n");

        // Suite 2: Sequential Latency (50 transactions) - clean un-queued latency
        System.out.println("[Suite 2/3] Running Sequential Latency Suite (50 transactions)...");
        BenchmarkResult seq50 = runSequentialSuite(client, "Sequential-50", 50, false);
        results.add(seq50);
        printSuiteResult(seq50);

        // Suite 3: Sustained Sequential (100 transactions)
        System.out.println("[Suite 3/3] Running Sustained Sequential Suite (100 transactions)...");
        BenchmarkResult seq100 = runSequentialSuite(client, "Sequential-100", 100, false);
        results.add(seq100);
        printSuiteResult(seq100);

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

                if (!success) {
                    System.err.println("  [Warning] Transaction " + txId + " unexpected response: resp1=" + resp1 + ", resp2=" + resp2);
                }

                records.add(new LatencyRecord(
                        txId,
                        tIngress - tSend,
                        tAudited - tIngress,
                        tAudited - tSend,
                        success
                ));

                if (!isWarmup && (i + 1) % 10 == 0) {
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
        System.out.printf("  Suite: %s | Count: %d | Success: %d/%d (%.1f%%) | Throughput: %.2f tx/sec\n",
                r.suiteName(), r.totalTransactions(), r.successfulTransactions(), r.totalTransactions(),
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
        System.out.println("Suite          Total   Success   Throughput    Ingress p50     DB Audit p50       E2E p50");
        System.out.println("--------------------------------------------------------------------------------");
        for (BenchmarkResult r : results) {
            System.out.printf("%-14s %5d   %7s   %8.2f/s   %9.3f ms   %10.2f ms   %9.2f ms\n",
                    r.suiteName(),
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
            writer.write("[\n");
            for (int i = 0; i < results.size(); i++) {
                BenchmarkResult r = results.get(i);
                writer.write(String.format(Locale.US,
                        "  {\n    \"suite\": \"%s\",\n    \"count\": %d,\n    \"success\": %d,\n    \"throughput\": %.2f,\n" +
                                "    \"ingress_p50\": %.3f,\n    \"ingress_p99\": %.3f,\n" +
                                "    \"db_p50\": %.2f,\n    \"db_p99\": %.2f,\n" +
                                "    \"e2e_p50\": %.2f,\n    \"e2e_p99\": %.2f\n  }%s\n",
                        r.suiteName(), r.totalTransactions(), r.successfulTransactions(), r.throughputOpsPerSec(),
                        r.ingressPercentiles().p50Ms(), r.ingressPercentiles().p99Ms(),
                        r.dbAuditPercentiles().p50Ms(), r.dbAuditPercentiles().p99Ms(),
                        r.e2ePercentiles().p50Ms(), r.e2ePercentiles().p99Ms(),
                        (i < results.size() - 1 ? "," : "")
                ));
            }
            writer.write("]\n");
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
