package org.g12.audit.ipc;

import java.nio.charset.StandardCharsets;

/** Shared-memory ABI for the C++ journal ring and Java ACK ring. */
public final class AuditRingProtocol {
    public static final byte[] MAGIC = "G12RING1".getBytes(StandardCharsets.US_ASCII);
    public static final int VERSION = 1;

    public static final int HEADER_OFFSET = 0;
    public static final int PRODUCER_SEQUENCE_OFFSET = 64;
    public static final int CONSUMER_SEQUENCE_OFFSET = 128;
    public static final int SLOTS_OFFSET = 192;

    public static final int RING_KIND_JOURNAL = 2;
    public static final int RING_KIND_ACK = 3;
    // Uses 8 bytes from the journal header reserved area; C++ leaves this area untouched.
    public static final int JOURNAL_STREAM_ID_OFFSET = 32;

    public static final int JOURNAL_SLOT_SIZE = 64;
    public static final int JOURNAL_CAPACITY = 1_048_576;
    public static final long JOURNAL_FILE_SIZE =
            SLOTS_OFFSET + (long) JOURNAL_SLOT_SIZE * JOURNAL_CAPACITY;

    public static final int ACK_SLOT_SIZE = 64;
    public static final int ACK_CAPACITY = 65_536;
    public static final long ACK_FILE_SIZE =
            SLOTS_OFFSET + (long) ACK_SLOT_SIZE * ACK_CAPACITY;

    // JournalSlot offsets, defined by sequencer/include/sequencer/ring_layout.hpp.
    public static final int JOURNAL_SEQUENCE_OFFSET = 0;
    public static final int JOURNAL_TIMESTAMP_TSC_OFFSET = 8;
    public static final int JOURNAL_TRANSACTION_ID_OFFSET = 16;
    public static final int JOURNAL_CONNECTION_ID_OFFSET = 24;
    public static final int JOURNAL_FROM_ACCOUNT_OFFSET = 32;
    public static final int JOURNAL_TO_ACCOUNT_OFFSET = 40;
    public static final int JOURNAL_AMOUNT_OFFSET = 48;
    public static final int JOURNAL_TYPE_OFFSET = 56;
    public static final int JOURNAL_STATUS_OFFSET = 57;
    public static final int JOURNAL_RESERVED_OFFSET = 58;
    public static final int JOURNAL_CHECKSUM_OFFSET = 60;
    public static final int JOURNAL_CHECKSUM_LENGTH = 60;

    // ACK record is a new, separate 64-byte ABI.
    public static final int ACK_JOURNAL_SEQUENCE_OFFSET = 0;
    public static final int ACK_TRANSACTION_ID_OFFSET = 8;
    public static final int ACK_CONNECTION_ID_OFFSET = 16;
    public static final int ACK_AUDITED_AT_EPOCH_MILLIS_OFFSET = 24;
    public static final int ACK_AUDIT_STATUS_OFFSET = 32;
    public static final int ACK_JOURNAL_STATUS_OFFSET = 36;
    public static final int ACK_RESERVED_OFFSET = 40;
    public static final int ACK_CHECKSUM_OFFSET = 60;
    public static final int ACK_CHECKSUM_LENGTH = 60;

    public static final int AUDIT_STATUS_PERSISTED = 1;

    public static final int JOURNAL_STATUS_SEQUENCED_OK = 0;
    public static final int JOURNAL_STATUS_REJECTED_MALFORMED = 1;

    static {
        if (MAGIC.length != 8
                || (JOURNAL_CAPACITY & (JOURNAL_CAPACITY - 1)) != 0
                || (ACK_CAPACITY & (ACK_CAPACITY - 1)) != 0) {
            throw new ExceptionInInitializerError("Invalid G12 audit ring constants");
        }
    }

    private AuditRingProtocol() {
    }
}
