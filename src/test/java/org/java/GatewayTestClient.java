
package org.java;

import com.google.flatbuffers.FlatBufferBuilder;

import org.java.schema.TransactionData;
import org.java.schema.TransactionMessage;
import org.java.schema.TransactionType;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.CRC32C;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Temporary integration-test client for the Netty ingress gateway.
 *
 * Run GatewayServer first, then run this class.
 */
public final class GatewayTestClient {

    private static final URI GATEWAY_URI = URI.create(
            System.getenv().getOrDefault("G12_GATEWAY_URI", "ws://localhost:8080/ws")
    );

    private static final int JOURNAL_OFFSET_SLOTS = RingProtocol.OFFSET_SLOTS;
    private static final int JOURNAL_SLOT_SIZE = RingProtocol.JOURNAL_SLOT_SIZE;
    private static final int JOURNAL_CAPACITY = RingProtocol.JOURNAL_CAPACITY;
    private static final VarHandle LONG_VIEW =
            MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    public static void main(String[] args) throws Exception {

        HttpClient client = HttpClient.newHttpClient();

        // If configured (as in Docker Compose), inspect the journal ring as an
        // observer. The test never advances consumerSeq; Audit Service owns it.
        String journalPathValue = System.getenv("G12_JOURNAL_RING_PATH");
        JournalProbe journalProbe = journalPathValue == null || journalPathValue.isBlank()
                ? null
                : new JournalProbe(Path.of(journalPathValue));

        try {
            // Test 1: A valid transfer between two different customer accounts.
            long journalSequenceBefore = journalProbe == null
                    ? -1L
                    : journalProbe.producerSequence();
            byte[] validTransaction = buildTransaction(
                    1001L,                 // Transaction ID
                    2500L,                 // Amount in smallest currency unit
                    101L,                  // Source account
                    202L,                  // Destination account
                    TransactionType.TRANSFER
            );

            List<String> validResponses = send(client, validTransaction, 2);

            check(
                    "Valid transfer ingress response",
                    validResponses.get(0),
                    "ACCEPT|stage=INGRESS|transactionId=1001"
            );
            check(
                    "Final audit acknowledgement",
                    validResponses.get(1),
                    "ACK|stage=AUDITED|transactionId=1001|journalSequence="
            );
            if (!validResponses.get(1).contains("|status=PERSISTED")) {
                throw new AssertionError("Audit ACK did not report PERSISTED: " + validResponses.get(1));
            }

            if (journalProbe != null) {
                journalProbe.awaitAndVerifyTransfer(journalSequenceBefore, 1001L, 2500L, 101L, 202L);
            }

            // Test 2: Zero amount must be rejected by business validation.
            byte[] invalidAmount = buildTransaction(
                    1002L,
                    0L,
                    101L,
                    202L,
                    TransactionType.TRANSFER
            );

            String invalidResponse = send(client, invalidAmount, 1).get(0);

            check(
                    "Zero amount",
                    invalidResponse,
                    "REJECT|code=INVALID_AMOUNT|"
            );

            // Test 3: Correct identifier, but an invalid root-table offset.
            byte[] malformedPayload = buildMalformedPayload();

            String malformedResponse = send(client, malformedPayload, 1).get(0);

            check(
                    "Malformed FlatBuffer",
                    malformedResponse,
                    "REJECT|code=MALFORMED_FLATBUFFER|"
            );

            System.out.println("\nAll gateway + sequencer + audit + ACK tests passed.");
        } finally {
            if (journalProbe != null) {
                journalProbe.close();
            }
        }
    }

    /**
     * Read-only integration-test observer for the Sequencer's journal output.
     * It deliberately does not advance journal.consumerSeq; that counter belongs
     * exclusively to the Audit Service.
     */
    private static final class JournalProbe implements AutoCloseable {
        private final FileChannel channel;
        private final MappedByteBuffer journal;

        private JournalProbe(Path path) throws Exception {
            long expectedSize = JOURNAL_OFFSET_SLOTS + (long) JOURNAL_CAPACITY * JOURNAL_SLOT_SIZE;
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
            if (channel.size() < expectedSize) {
                channel.close();
                throw new IllegalStateException(
                        "Journal ring is truncated: expected " + expectedSize + " bytes"
                );
            }
            this.journal = channel.map(FileChannel.MapMode.READ_ONLY, 0, expectedSize);
            this.journal.order(ByteOrder.LITTLE_ENDIAN);

            byte[] magic = new byte[8];
            for (int i = 0; i < magic.length; i++) {
                magic[i] = journal.get(i);
            }
            if (!Arrays.equals(magic, "G12RING1".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
                    || journal.getInt(8) != RingProtocol.RING_VERSION
                    || journal.getInt(12) != JOURNAL_SLOT_SIZE
                    || journal.getLong(16) != JOURNAL_CAPACITY
                    || journal.getInt(24) != RingProtocol.RING_KIND_JOURNAL) {
                channel.close();
                throw new IllegalStateException("Journal ring header does not match the protocol");
            }
        }

        private long producerSequence() {
            return (long) LONG_VIEW.getAcquire(journal, 64);
        }

        private void awaitAndVerifyTransfer(
                long expectedSequence,
                long transactionId,
                long amount,
                long fromAccount,
                long toAccount
        ) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                if (Long.compareUnsigned(producerSequence(), expectedSequence + 1) >= 0) {
                    break;
                }
                Thread.sleep(1);
            }

            if (Long.compareUnsigned(producerSequence(), expectedSequence + 1) < 0) {
                throw new AssertionError("C++ Sequencer did not publish the expected journal record");
            }

            int slotOffset = JOURNAL_OFFSET_SLOTS
                    + (int) (expectedSequence & (JOURNAL_CAPACITY - 1L)) * JOURNAL_SLOT_SIZE;

            long actualSequence = journal.getLong(slotOffset);
            long actualTransactionId = journal.getLong(slotOffset + 16);
            long connectionId = journal.getLong(slotOffset + 24);
            long actualFrom = journal.getLong(slotOffset + 32);
            long actualTo = journal.getLong(slotOffset + 40);
            long actualAmount = journal.getLong(slotOffset + 48);
            int actualType = Byte.toUnsignedInt(journal.get(slotOffset + 56));
            int actualStatus = Byte.toUnsignedInt(journal.get(slotOffset + 57));
            int actualChecksum = journal.getInt(slotOffset + 60);

            CRC32C crc32c = new CRC32C();
            ByteBuffer checksumBytes = journal.duplicate();
            checksumBytes.position(slotOffset).limit(slotOffset + 60);
            crc32c.update(checksumBytes.slice());
            int expectedChecksum = (int) crc32c.getValue();

            if (actualSequence != expectedSequence
                    || actualTransactionId != transactionId
                    || connectionId <= 0
                    || actualFrom != fromAccount
                    || actualTo != toAccount
                    || actualAmount != amount
                    || actualType != 0
                    || actualStatus != 0
                    || actualChecksum != expectedChecksum) {
                throw new AssertionError(
                        "Journal record verification failed: seq=" + actualSequence
                                + ", txn=" + actualTransactionId
                                + ", from=" + actualFrom
                                + ", to=" + actualTo
                                + ", amount=" + actualAmount
                                + ", type=" + actualType
                                + ", status=" + actualStatus
                                + ", checksumValid=" + (actualChecksum == expectedChecksum)
                );
            }

            System.out.println(
                    "C++ journal record verified: sequence=" + actualSequence
                            + ", transactionId=" + actualTransactionId
                            + ", CRC32C=valid"
            );
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

    /**
     * Builds a real FlatBuffers message using the generated schema classes.
     */
    private static byte[] buildTransaction(
            long transactionId,
            long amount,
            long fromAccount,
            long toAccount,
            int type
    ) {
        FlatBufferBuilder builder = new FlatBufferBuilder(128);

        /*
         * The TransactionData struct must be created inline while its
         * containing table is being built.
         */
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

        int rootOffset =
                TransactionMessage.endTransactionMessage(builder);

        /*
         * This generated helper finishes the buffer with the schema's
         * configured file identifier, "NIGT".
         */
        TransactionMessage.finishTransactionMessageBuffer(
                builder,
                rootOffset
        );

        // Return the final serialized FlatBuffers payload.
        return builder.sizedByteArray();
    }

    /**
     * Starts with a genuine FlatBuffers message and corrupts only its root offset.
     * This preserves the actual file identifier created by FlatBufferBuilder.
     */
    private static byte[] buildMalformedPayload() {

        // Generate a valid message using our actual schema.
        byte[] payload = buildTransaction(
                1003L,
                2500L,
                101L,
                202L,
                TransactionType.TRANSFER
        );

        /*
         * FlatBuffers stores the root-table offset in the first four bytes.
         * Replace it with an offset outside the payload, leaving the file
         * identifier and the rest of the payload unchanged.
         */
        ByteBuffer.wrap(payload)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(0, payload.length + 1024);

        return payload;
    }


    /** Opens a WebSocket, sends one binary message, and awaits the requested responses. */
    private static List<String> send(
            HttpClient client,
            byte[] payload,
            int expectedResponses
    ) throws Exception {
        BlockingQueue<String> responses = new LinkedBlockingQueue<>();
        StringBuilder receivedText = new StringBuilder();
        AtomicBoolean opened = new AtomicBoolean(false);

        WebSocket.Listener listener = new WebSocket.Listener() {
            @Override
            public void onOpen(WebSocket webSocket) {
                opened.set(true);
                webSocket.request(1);
            }

            @Override
            public CompletableFuture<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                receivedText.append(data);
                if (last) {
                    responses.offer(receivedText.toString());
                    receivedText.setLength(0);
                }
                webSocket.request(1);
                return null;
            }

            @Override
            public void onError(WebSocket webSocket, Throwable error) {
                if (opened.get()) {
                    responses.offer("__ERROR__" + error);
                }
            }
        };

        WebSocket webSocket = connectWithRetry(client, listener);
        try {
            webSocket.sendBinary(ByteBuffer.wrap(payload), true).join();
            List<String> received = new ArrayList<>(expectedResponses);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (received.size() < expectedResponses && System.nanoTime() < deadline) {
                String response = responses.poll(250, TimeUnit.MILLISECONDS);
                if (response != null) {
                    if (response.startsWith("__ERROR__")) {
                        throw new IllegalStateException("WebSocket failed: " + response);
                    }
                    received.add(response);
                }
            }
            if (received.size() != expectedResponses) {
                throw new AssertionError("Expected " + expectedResponses
                        + " WebSocket responses but received " + received);
            }
            return received;
        } finally {
            webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "Test complete").join();
        }
    }


    /**
     * Compose starts the test container once the gateway container has started,
     * not necessarily once Netty has bound its port. Retry connection briefly.
     */
    private static WebSocket connectWithRetry(
            HttpClient client,
            WebSocket.Listener listener
    ) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        Throwable lastFailure = null;

        while (System.nanoTime() < deadline) {
            try {
                return client.newWebSocketBuilder()
                        .connectTimeout(java.time.Duration.ofSeconds(3))
                        .buildAsync(GATEWAY_URI, listener)
                        .get(4, TimeUnit.SECONDS);
            } catch (Exception failure) {
                lastFailure = failure;
                Thread.sleep(250);
            }
        }

        throw new IllegalStateException(
                "Unable to connect to gateway at " + GATEWAY_URI,
                lastFailure
        );
    }

    /**
     * Prints the actual response and fails fast if it differs from expectation.
     */
    private static void check(
            String testName,
            String actual,
            String expected
    ) {
        System.out.println(testName + ": " + actual);

        if (!actual.startsWith(expected)) {
            throw new AssertionError(
                    testName + " failed. Expected prefix: "
                            + expected + ", actual: " + actual
            );
        }

        System.out.println("  PASS");
    }
}

