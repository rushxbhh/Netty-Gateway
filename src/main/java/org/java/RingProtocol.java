package org.java;

/**
 * Java-side constants for the shared-memory ABI defined by
 * sequencer/include/sequencer/ring_layout.hpp.
 *
 * Keep these values byte-for-byte aligned with the C++ header. In particular,
 * the 96-byte inbound payload limit and the 64-byte journal record size belong
 * to two different rings and must not be treated as competing limits.
 */
public final class RingProtocol {

    public static final String RING_MAGIC_ASCII = "G12RING1";
    public static final int RING_VERSION = 1;

    public static final int RING_KIND_INBOUND = 1;
    public static final int RING_KIND_JOURNAL = 2;

    public static final int OFFSET_HEADER = 0;
    public static final int OFFSET_PRODUCER_SEQUENCE = 64;
    public static final int OFFSET_CONSUMER_SEQUENCE = 128;
    public static final int OFFSET_SLOTS = 192;

    // Inbound ring: 32 bytes of slot metadata + 96 bytes of FlatBuffer payload.
    public static final int INBOUND_METADATA_SIZE = 32;
    public static final int INBOUND_CLIENT_TXN_ID_OFFSET = 16;
    public static final int INBOUND_SLOT_SIZE = 128;
    public static final int INBOUND_CAPACITY = 65_536;
    public static final int INBOUND_PAYLOAD_MAX = 96;

    // Separate downstream journal ring: one fixed 64-byte sequencer record.
    public static final int JOURNAL_SLOT_SIZE = 64;
    public static final int JOURNAL_CAPACITY = 1_048_576;

    public static final long INBOUND_FILE_SIZE =
            OFFSET_SLOTS + (long) INBOUND_CAPACITY * INBOUND_SLOT_SIZE;
    public static final long JOURNAL_FILE_SIZE =
            OFFSET_SLOTS + (long) JOURNAL_CAPACITY * JOURNAL_SLOT_SIZE;

    static final byte[] RING_MAGIC_BYTES = RING_MAGIC_ASCII.getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    static {
        if (RING_MAGIC_BYTES.length != 8
                || INBOUND_SLOT_SIZE != INBOUND_METADATA_SIZE + INBOUND_PAYLOAD_MAX
                || (INBOUND_CAPACITY & (INBOUND_CAPACITY - 1)) != 0
                || (JOURNAL_CAPACITY & (JOURNAL_CAPACITY - 1)) != 0) {
            throw new ExceptionInInitializerError("Invalid G12 shared-memory ring protocol constants");
        }
    }

    private RingProtocol() {
    }
}
