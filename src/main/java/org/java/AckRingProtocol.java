package org.java;

import java.nio.charset.StandardCharsets;

/** Gateway-side mirror of audit-service's 64-byte ACK ring ABI. */
public final class AckRingProtocol {
    public static final byte[] MAGIC = "G12RING1".getBytes(StandardCharsets.US_ASCII);
    public static final int VERSION = 1;
    public static final int RING_KIND_ACK = 3;
    public static final int PRODUCER_OFFSET = 64;
    public static final int CONSUMER_OFFSET = 128;
    public static final int SLOTS_OFFSET = 192;
    public static final int SLOT_SIZE = 64;
    public static final int CAPACITY = 65_536;
    public static final long FILE_SIZE = SLOTS_OFFSET + (long) SLOT_SIZE * CAPACITY;

    public static final int AUDIT_STATUS_PERSISTED = 1;

    private AckRingProtocol() {
    }
}
