
package org.java;

import org.java.schema.TransactionData;
import org.java.schema.TransactionMessage;
import org.java.schema.TransactionType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Validates FlatBuffers payloads before the gateway accepts them.
 *
 * This class performs ingress validation only. It does not check balances,
 * persist transactions, or provide duplicate-transaction protection.
 */
public final class TransactionValidator {

    // Limit payload allocation and parsing work at the ingress boundary.
    // Keep this aligned with the Netty WebSocket frame limits.
    public static final int MAX_PAYLOAD_BYTES = 64 * 1024;

    // A FlatBuffer with a file identifier needs at least a root offset and identifier.
    private static final int MIN_PAYLOAD_BYTES = 8;


    public GatewayResponse validate(ByteBuffer payload) {

        // Reject missing input before accessing the buffer.
        if (payload == null) {
            return GatewayResponse.reject(
                    "EMPTY_PAYLOAD",
                    "Payload is missing"
            );
        }

        /*
         * slice() creates a view of the remaining bytes without copying the payload.
         * Explicitly set little-endian order because FlatBuffers uses little-endian.
         *
         * The original buffer's position and limit remain unchanged.
         */
        ByteBuffer buffer = payload.slice().order(ByteOrder.LITTLE_ENDIAN);

        // remaining() represents the bytes available between position and limit.
        int payloadSize = buffer.remaining();

        // A root offset plus a four-byte file identifier requires at least 8 bytes.
        if (payloadSize < MIN_PAYLOAD_BYTES) {
            return GatewayResponse.reject(
                    "PAYLOAD_TOO_SMALL",
                    "Payload is too small to contain a FlatBuffer"
            );
        }

        // Reject oversized input before attempting to decode it.
        if (payloadSize > MAX_PAYLOAD_BYTES) {
            return GatewayResponse.reject(
                    "PAYLOAD_TOO_LARGE",
                    "Payload exceeds the maximum allowed size"
            );
        }

        try {
            // Verify that this payload uses our expected FlatBuffers schema identifier.
            if (!TransactionMessage.TransactionMessageBufferHasIdentifier(buffer)) {
                return GatewayResponse.reject(
                        "INVALID_IDENTIFIER",
                        "Payload does not contain the expected G12T identifier"
                );
            }

            // Ensure the generated classes and FlatBuffers runtime are version-compatible.
            TransactionMessage.ValidateVersion();

            // Read the generated FlatBuffers object directly from the buffer.
            TransactionMessage message =
                    TransactionMessage.getRootAsTransactionMessage(buffer);

            // Retrieve the transaction struct embedded in the root table.
            TransactionData transaction = message.transaction();

            // Reject a missing transaction instead of validating absent data.
            if (transaction == null) {
                return GatewayResponse.reject(
                        "MISSING_TRANSACTION",
                        "Transaction data is missing"
                );
            }

            // Apply the existing business-field validation rules.
            return validateFields(transaction);

        } catch (RuntimeException exception) {

            // Malformed or truncated data must not escape into the Netty event loop.
            return GatewayResponse.reject(
                    "MALFORMED_FLATBUFFER",
                    "Payload could not be safely decoded as a transaction"
            );
        }
    }


    private GatewayResponse validateFields(TransactionData transaction) {

        long transactionId = transaction.transactionId();
        long amount = transaction.amount();
        long fromAccount = transaction.fromAccount();
        long toAccount = transaction.toAccount();
        byte type = (byte) transaction.type();

        // Reject zero or negative IDs; negative values can also indicate an
        // unsigned FlatBuffers ulong that exceeds Java's signed long range.
        if (transactionId <= 0) {
            return GatewayResponse.reject(
                    "INVALID_TRANSACTION_ID",
                    "Transaction ID must be positive and within the supported range"
            );
        }

        /*
         * Amount is an integer in the smallest currency unit, not a floating-point
         * number. For example, 1250 could represent 12.50 in a two-decimal currency.
         */
        if (amount <= 0) {
            return GatewayResponse.reject(
                    "INVALID_AMOUNT",
                    "Amount must be positive"
            );
        }

        /*
         * Account fields use FlatBuffers ulong, which maps to Java long.
         * This implementation supports non-negative signed-long values only.
         */
        if (fromAccount < 0 || toAccount < 0) {
            return GatewayResponse.reject(
                    "INVALID_ACCOUNT_ID",
                    "Account IDs are outside the supported range"
            );
        }

        // Dispatch to type-specific checks after common validation succeeds.
        return switch (type) {
            case TransactionType.TRANSFER ->
                    validateTransfer(transactionId, fromAccount, toAccount);

            case TransactionType.DEPOSIT ->
                    validateDeposit(transactionId, fromAccount, toAccount);

            case TransactionType.WITHDRAWAL ->
                    validateWithdrawal(transactionId, fromAccount, toAccount);

            default -> GatewayResponse.reject(
                    "INVALID_TRANSACTION_TYPE",
                    "Transaction type is not supported"
            );
        };
    }

    private GatewayResponse validateTransfer(
            long transactionId,
            long fromAccount,
            long toAccount
    ) {
        // System account 0 is reserved for deposits/withdrawals, not transfers.
        if (fromAccount == 0 || toAccount == 0) {
            return GatewayResponse.reject(
                    "INVALID_TRANSFER_ACCOUNT",
                    "A transfer requires two non-system accounts"
            );
        }

        // A transfer from an account to itself is not accepted by this gateway.
        if (fromAccount == toAccount) {
            return GatewayResponse.reject(
                    "SAME_ACCOUNT_TRANSFER",
                    "Source and destination accounts must differ"
            );
        }

        return GatewayResponse.accept(transactionId);
    }

    private GatewayResponse validateDeposit(
            long transactionId,
            long fromAccount,
            long toAccount
    ) {
        // In this schema, a deposit is represented as system account 0 -> customer.
        if (fromAccount != 0 || toAccount == 0) {
            return GatewayResponse.reject(
                    "INVALID_DEPOSIT_ACCOUNT",
                    "A deposit requires system account 0 as source and a customer destination"
            );
        }

        return GatewayResponse.accept(transactionId);
    }

    private GatewayResponse validateWithdrawal(
            long transactionId,
            long fromAccount,
            long toAccount
    ) {
        // In this schema, a withdrawal is represented as customer -> system account 0.
        if (fromAccount == 0 || toAccount != 0) {
            return GatewayResponse.reject(
                    "INVALID_WITHDRAWAL_ACCOUNT",
                    "A withdrawal requires a customer source and system account 0 as destination"
            );
        }

        return GatewayResponse.accept(transactionId);
    }
}
