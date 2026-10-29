
package org.java;

import com.google.flatbuffers.FlatBufferBuilder;

import org.java.schema.TransactionData;
import org.java.schema.TransactionMessage;
import org.java.schema.TransactionType;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Temporary integration-test client for the Netty ingress gateway.
 *
 * Run GatewayServer first, then run this class.
 */
public final class GatewayTestClient {

    private static final URI GATEWAY_URI =
            URI.create("ws://localhost:8080/ws");

    public static void main(String[] args) throws Exception {

        HttpClient client = HttpClient.newHttpClient();

        // Test 1: A valid transfer between two different customer accounts.
        byte[] validTransaction = buildTransaction(
                1001L,                 // Transaction ID
                2500L,                 // Amount in smallest currency unit
                101L,                  // Source account
                202L,                  // Destination account
                TransactionType.TRANSFER
        );

        String validResponse = send(client, validTransaction);

        check(
                "Valid transfer",
                validResponse,
                "ACCEPT|stage=VALIDATION|transactionId=1001"
        );

        // Test 2: Zero amount must be rejected by business validation.
        byte[] invalidAmount = buildTransaction(
                1002L,
                0L,
                101L,
                202L,
                TransactionType.TRANSFER
        );

        String invalidResponse = send(client, invalidAmount);

        check(
                "Zero amount",
                invalidResponse,
                "REJECT|code=INVALID_AMOUNT|"
        );

        // Test 3: Correct identifier, but an invalid root-table offset.
        byte[] malformedPayload = buildMalformedPayload();

        String malformedResponse = send(client, malformedPayload);

        check(
                "Malformed FlatBuffer",
                malformedResponse,
                "REJECT|code=MALFORMED_FLATBUFFER|"
        );

        System.out.println("\nAll three gateway tests passed.");
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
         * configured file identifier, "G12T".
         */
        TransactionMessage.finishTransactionMessageBuffer(
                builder,
                rootOffset
        );

        // Return the final serialized FlatBuffers payload.
        return builder.sizedByteArray();
    }

    /**
     * Creates an eight-byte payload with a valid G12T identifier but an
     * impossible root-table offset, so parsing should fail.
     */

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


    /**
     * Opens a WebSocket, sends one binary message, and awaits one text response.
     */
    private static String send(
            HttpClient client,
            byte[] payload
    ) throws Exception {

        CompletableFuture<String> responseFuture =
                new CompletableFuture<>();

        StringBuilder receivedText = new StringBuilder();

        WebSocket.Listener listener = new WebSocket.Listener() {

            @Override
            public void onOpen(WebSocket webSocket) {
                // Request permission to receive the next WebSocket event.
                webSocket.request(1);
            }

            @Override
            public CompletableFuture<?> onText(
                    WebSocket webSocket,
                    CharSequence data,
                    boolean last
            ) {
                // Accumulate response fragments into one message.
                receivedText.append(data);

                if (last) {
                    responseFuture.complete(receivedText.toString());
                }

                // Continue receiving WebSocket events.
                webSocket.request(1);
                return null;
            }

            @Override
            public void onError(
                    WebSocket webSocket,
                    Throwable error
            ) {
                responseFuture.completeExceptionally(error);
            }
        };

        // Establish a WebSocket connection to the running gateway.
        WebSocket webSocket = client.newWebSocketBuilder()
                .buildAsync(GATEWAY_URI, listener)
                .join();

        try {
            // Send the serialized FlatBuffers payload as a binary message.
            webSocket.sendBinary(ByteBuffer.wrap(payload), true).join();

            // Fail the test rather than waiting indefinitely for a response.
            return responseFuture.get(5, TimeUnit.SECONDS);

        } finally {
            // Close this test connection after the response or on failure.
            webSocket.sendClose(
                    WebSocket.NORMAL_CLOSURE,
                    "Test complete"
            ).join();
        }
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

