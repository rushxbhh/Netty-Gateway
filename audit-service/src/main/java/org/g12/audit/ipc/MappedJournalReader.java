package org.g12.audit.ipc;

import org.g12.audit.model.JournalRecord;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.security.SecureRandom;

/**
 * Single-consumer view of the C++ journal ring. The Audit Service owns only
 * consumerSeq; the C++ Sequencer exclusively owns producerSeq.
 */
public final class MappedJournalReader implements AutoCloseable {
    private static final VarHandle LONG_VIEW = MethodHandles.byteBufferViewVarHandle(
            long[].class, ByteOrder.LITTLE_ENDIAN);

    private final FileChannel channel;
    private final MappedByteBuffer ring;
    private final long streamId;

    public MappedJournalReader(Path path) throws IOException {
        requireLittleEndian();
        this.channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            FileLock startupLock = channel.lock();
            try {
                if (channel.size() < AuditRingProtocol.JOURNAL_FILE_SIZE) {
                    throw new IOException("Journal ring is missing or truncated: " + path);
                }
                this.ring = channel.map(FileChannel.MapMode.READ_WRITE, 0,
                        AuditRingProtocol.JOURNAL_FILE_SIZE);
                ring.order(ByteOrder.LITTLE_ENDIAN);
                validateHeader();
                validateCounters();
                this.streamId = loadOrCreateStreamId();
            } finally {
                startupLock.release();
            }
        } catch (Throwable failure) {
            try { channel.close(); } catch (IOException closeFailure) { failure.addSuppressed(closeFailure); }
            if (failure instanceof IOException io) throw io;
            if (failure instanceof RuntimeException runtime) throw runtime;
            throw new IOException("Unable to map journal ring", failure);
        }
    }

    /** Identifies one incarnation of the volatile journal ring across database restarts. */
    public long streamId() {
        return streamId;
    }

    public long producerSequence() {
        return (long) LONG_VIEW.getAcquire(ring, AuditRingProtocol.PRODUCER_SEQUENCE_OFFSET);
    }

    public long consumerSequence() {
        return (long) LONG_VIEW.get(ring, AuditRingProtocol.CONSUMER_SEQUENCE_OFFSET);
    }

    /** Returns null when no published journal record is currently available. */
    public JournalRecord readNext() throws IOException {
        long consumer = consumerSequence();
        long producer = producerSequence();
        long used = producer - consumer;
        if (Long.compareUnsigned(used, AuditRingProtocol.JOURNAL_CAPACITY) > 0) {
            throw new IOException("Journal counters are inconsistent: producer="
                    + Long.toUnsignedString(producer) + ", consumer=" + Long.toUnsignedString(consumer));
        }
        if (consumer == producer) return null;

        int slotOffset = slotOffset(consumer);
        long sequence = ring.getLong(slotOffset + AuditRingProtocol.JOURNAL_SEQUENCE_OFFSET);
        if (sequence != consumer) {
            throw new IOException("Journal sequence mismatch: expected "
                    + Long.toUnsignedString(consumer) + " but slot contains "
                    + Long.toUnsignedString(sequence));
        }

        int storedChecksum = ring.getInt(slotOffset + AuditRingProtocol.JOURNAL_CHECKSUM_OFFSET);
        int actualChecksum = JournalChecksum.crc32c(
                ring, slotOffset, AuditRingProtocol.JOURNAL_CHECKSUM_LENGTH);
        if (storedChecksum != actualChecksum) {
            throw new IOException("CRC32C mismatch for journal sequence "
                    + Long.toUnsignedString(sequence));
        }

        return new JournalRecord(
                sequence,
                ring.getLong(slotOffset + AuditRingProtocol.JOURNAL_TIMESTAMP_TSC_OFFSET),
                ring.getLong(slotOffset + AuditRingProtocol.JOURNAL_TRANSACTION_ID_OFFSET),
                ring.getLong(slotOffset + AuditRingProtocol.JOURNAL_CONNECTION_ID_OFFSET),
                ring.getLong(slotOffset + AuditRingProtocol.JOURNAL_FROM_ACCOUNT_OFFSET),
                ring.getLong(slotOffset + AuditRingProtocol.JOURNAL_TO_ACCOUNT_OFFSET),
                ring.getLong(slotOffset + AuditRingProtocol.JOURNAL_AMOUNT_OFFSET),
                Byte.toUnsignedInt(ring.get(slotOffset + AuditRingProtocol.JOURNAL_TYPE_OFFSET)),
                Byte.toUnsignedInt(ring.get(slotOffset + AuditRingProtocol.JOURNAL_STATUS_OFFSET)),
                Short.toUnsignedInt(ring.getShort(slotOffset + AuditRingProtocol.JOURNAL_RESERVED_OFFSET)),
                storedChecksum
        );
    }

    /** Publish completion only after persistence and ACK publication have succeeded. */
    public void advanceConsumer(long expectedSequence) throws IOException {
        long current = consumerSequence();
        if (current != expectedSequence) {
            throw new IOException("Journal consumer changed unexpectedly: expected "
                    + expectedSequence + ", found " + current);
        }
        LONG_VIEW.setRelease(ring, AuditRingProtocol.CONSUMER_SEQUENCE_OFFSET, expectedSequence + 1);
    }


    private long loadOrCreateStreamId() throws IOException {
        long existing = ring.getLong(AuditRingProtocol.JOURNAL_STREAM_ID_OFFSET);
        if (existing != 0) return existing;

        // The field is in RingHeaderLine.reserved[36], which C++ does not read or
        // write. Initialize it once under the startup file lock. A fresh /dev/shm
        // journal gets a new ID; a service restart on the same ring preserves it.
        long generated = new SecureRandom().nextLong() & Long.MAX_VALUE;
        if (generated == 0) generated = 1;
        ring.putLong(AuditRingProtocol.JOURNAL_STREAM_ID_OFFSET, generated);
        ring.force(AuditRingProtocol.JOURNAL_STREAM_ID_OFFSET, Long.BYTES);
        return generated;
    }

    private static int slotOffset(long sequence) {
        long index = sequence & (AuditRingProtocol.JOURNAL_CAPACITY - 1L);
        return Math.toIntExact(AuditRingProtocol.SLOTS_OFFSET
                + index * AuditRingProtocol.JOURNAL_SLOT_SIZE);
    }

    private void validateHeader() throws IOException {
        byte[] actual = new byte[8];
        for (int i = 0; i < actual.length; i++) actual[i] = ring.get(i);
        if (!Arrays.equals(actual, AuditRingProtocol.MAGIC)
                || ring.getInt(8) != AuditRingProtocol.VERSION
                || ring.getInt(12) != AuditRingProtocol.JOURNAL_SLOT_SIZE
                || ring.getLong(16) != AuditRingProtocol.JOURNAL_CAPACITY
                || ring.getInt(24) != AuditRingProtocol.RING_KIND_JOURNAL) {
            throw new IOException("Journal ring header/layout mismatch");
        }
    }

    private void validateCounters() throws IOException {
        long producer = producerSequence();
        long consumer = consumerSequence();
        if (Long.compareUnsigned(producer - consumer, AuditRingProtocol.JOURNAL_CAPACITY) > 0) {
            throw new IOException("Journal ring counters exceed capacity");
        }
    }

    private static void requireLittleEndian() throws IOException {
        if (ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN) {
            throw new IOException("G12 shared-memory protocol requires a little-endian host");
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
