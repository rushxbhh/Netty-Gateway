
package org.java;

/**
 * Represents the result returned by the gateway after validating a request.
 *
 * This is an application-level response, not a WebSocket protocol response.
 */
public final class GatewayResponse {

    // Indicates whether the incoming transaction passed gateway validation.
    private final boolean accepted;

    // Stable machine-readable result code, for example ACCEPTED or INVALID_AMOUNT.
    private final String code;

    // Human-readable explanation suitable for debugging or client display.
    private final String message;

    // Transaction ID is included in accepted responses.
    private final long transactionId;

    // Private constructor ensures responses are created through the factory methods.
    private GatewayResponse(
            boolean accepted,
            String code,
            String message,
            long transactionId
    ) {
        this.accepted = accepted;
        this.code = code;
        this.message = message;
        this.transactionId = transactionId;
    }

    /**
     * Creates a response when the transaction passes ingress validation.
     *
     * This does not mean the ledger has processed or committed it.
     */
    public static GatewayResponse accept(long transactionId) {
        return new GatewayResponse(
                true,
                "ACCEPTED",
                "Transaction passed ingress validation",
                transactionId
        );
    }

    /**
     * Creates a rejection response with a stable code and explanation.
     */
    public static GatewayResponse reject(String code, String message) {
        return new GatewayResponse(
                false,
                code,
                message,
                0L
        );
    }

    public boolean isAccepted() {
        return accepted;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public long getTransactionId() {
        return transactionId;
    }

    /**
     * Converts the response to a simple text protocol for WebSocket clients.
     *
     * Example:
     * ACCEPT|stage=VALIDATION|transactionId=123
     * REJECT|code=INVALID_AMOUNT|message=Amount must be positive
     */
    public String toWireText() {
        if (accepted) {
            return "ACCEPT|stage=VALIDATION|transactionId=" + transactionId;
        }

        // Prevent separators or line breaks from corrupting the response format.
        String safeCode = sanitize(code);
        String safeMessage = sanitize(message);

        return "REJECT|code=" + safeCode + "|message=" + safeMessage;
    }

    private static String sanitize(String value) {
        if (value == null) {
            return "";
        }

        return value
                .replace("|", "/")
                .replace("\n", " ")
                .replace("\r", " ");
    }
}

