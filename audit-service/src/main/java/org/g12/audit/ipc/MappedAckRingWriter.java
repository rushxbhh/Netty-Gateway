package org.g12.audit.ipc;

import org.g12.audit.model.AuditAcknowledgement;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.zip.CRC32C;

/** Single producer for the Audit -> Gateway acknowledgement ring. */
public final class MappedAckRingWriter implements AutoCloseable {
    private static final VarHandle LONG_VIEW = MethodHandles.byteBufferViewVarHandle(
            long[].class, ByteOrder.LITTLE_ENDIAN);

    private final FileChannel channel;
    private final MappedByteBuffer ring;

    public MappedAckRingWriter(Path path) throws IOException {
        if (ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN) {
            throw new IOException("G12 shared-memory protocol requires a little-endian host");
        }
        Path absolute = path.toAbsolutePath().normalize();
        Path parent = absolute.getParent();
        if (parent != null) Files.createDirectories(parent);
        channel = FileChannel.open(absolute, StandardOpenOption.CREATE,
                StandardOpenOption.READ, StandardOpenOption.WRITE);

        try {
            FileLock lock = channel.lock();
            try {
                long size = channel.size();
                boolean newFile = size == 0;
                if (newFile) {
                    channel.position(AuditRingProtocol.ACK_FILE_SIZE - 1);
                    channel.write(ByteBuffer.wrap(new byte[]{0}));
                } else if (size < AuditRingProtocol.ACK_FILE_SIZE) {
                    throw new IOException("ACK ring is truncated: expected at least "
                            + AuditRingProtocol.ACK_FILE_SIZE + " bytes, found " + size);
                }

                ring = channel.map(FileChannel.MapMode.READ_WRITE, 0,
                        AuditRingProtocol.ACK_FILE_SIZE);
                ring.order(ByteOrder.LITTLE_ENDIAN);
                if (newFile) initializeNewRing();
                else validateExistingRing();
            } finally {
                lock.release();
            }
        } catch (Throwable failure) {
            try { channel.close(); } catch (IOException closeFailure) { failure.addSuppressed(closeFailure); }
            if (failure instanceof IOException io) throw io;
            if (failure instanceof RuntimeException runtime) throw runtime;
            throw new IOException("Unable to initialize ACK ring", failure);
        }
    }

    /**
     * Waits for a free ACK slot. The journal consumer is not advanced until this
     * method returns, so a full ACK ring applies backpressure instead of losing ACKs.
     */
    public long publish(AuditAcknowledgement acknowledgement, BooleanSupplier running)
            throws IOException, InterruptedException {
        while (running.getAsBoolean() && !Thread.currentThread().isInterrupted()) {
            long producer = (long) LONG_VIEW.get(ring, AuditRingProtocol.PRODUCER_SEQUENCE_OFFSET);
            long consumer = (long) LONG_VIEW.getAcquire(ring, AuditRingProtocol.CONSUMER_SEQUENCE_OFFSET);
            long used = producer - consumer;
            if (Long.compareUnsigned(used, AuditRingProtocol.ACK_CAPACITY) > 0) {
                throw new IOException("ACK ring counters exceed capacity");
            }

            if (used < AuditRingProtocol.ACK_CAPACITY) {
                int slotOffset = slotOffset(producer);
                ring.putLong(slotOffset + AuditRingProtocol.ACK_JOURNAL_SEQUENCE_OFFSET,
                        acknowledgement.journalSequence());
                ring.putLong(slotOffset + AuditRingProtocol.ACK_TRANSACTION_ID_OFFSET,
                        acknowledgement.transactionId());
                ring.putLong(slotOffset + AuditRingProtocol.ACK_CONNECTION_ID_OFFSET,
                        acknowledgement.connectionId());
                ring.putLong(slotOffset + AuditRingProtocol.ACK_AUDITED_AT_EPOCH_MILLIS_OFFSET,
                        acknowledgement.auditedAtEpochMillis());
                ring.putInt(slotOffset + AuditRingProtocol.ACK_AUDIT_STATUS_OFFSET,
                        acknowledgement.auditStatus());
                ring.putInt(slotOffset + AuditRingProtocol.ACK_JOURNAL_STATUS_OFFSET,
                        acknowledgement.journalStatus());
                for (int i = AuditRingProtocol.ACK_RESERVED_OFFSET; i < 60; i++) {
                    ring.put(slotOffset + i, (byte) 0);
                }
                int checksum = crc32c(ring, slotOffset, AuditRingProtocol.ACK_CHECKSUM_LENGTH);
                ring.putInt(slotOffset + AuditRingProtocol.ACK_CHECKSUM_OFFSET, checksum);

                // Publication is last: Gateway must only inspect a complete slot.
                LONG_VIEW.setRelease(ring, AuditRingProtocol.PRODUCER_SEQUENCE_OFFSET, producer + 1);
                return producer;
            }

            LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
            if (Thread.interrupted()) throw new InterruptedException();
        }
        throw new InterruptedException("Audit service is stopping before ACK publication");
    }

    public long producerSequence() {
        return (long) LONG_VIEW.getAcquire(ring, AuditRingProtocol.PRODUCER_SEQUENCE_OFFSET);
    }

    public long consumerSequence() {
        return (long) LONG_VIEW.getAcquire(ring, AuditRingProtocol.CONSUMER_SEQUENCE_OFFSET);
    }

    private void initializeNewRing() {
        for (int i = 0; i < AuditRingProtocol.SLOTS_OFFSET; i++) ring.put(i, (byte) 0);
        ring.putInt(8, AuditRingProtocol.VERSION);
        ring.putInt(12, AuditRingProtocol.ACK_SLOT_SIZE);
        ring.putLong(16, AuditRingProtocol.ACK_CAPACITY);
        ring.putInt(24, AuditRingProtocol.RING_KIND_ACK);
        LONG_VIEW.setRelease(ring, AuditRingProtocol.PRODUCER_SEQUENCE_OFFSET, 0L);
        LONG_VIEW.setRelease(ring, AuditRingProtocol.CONSUMER_SEQUENCE_OFFSET, 0L);
        VarHandle.releaseFence();
        for (int i = 0; i < AuditRingProtocol.MAGIC.length; i++) {
            ring.put(i, AuditRingProtocol.MAGIC[i]);
        }
    }

    private void validateExistingRing() throws IOException {
        byte[] actual = new byte[8];
        for (int i = 0; i < actual.length; i++) actual[i] = ring.get(i);
        if (!Arrays.equals(actual, AuditRingProtocol.MAGIC)
                || ring.getInt(8) != AuditRingProtocol.VERSION
                || ring.getInt(12) != AuditRingProtocol.ACK_SLOT_SIZE
                || ring.getLong(16) != AuditRingProtocol.ACK_CAPACITY
                || ring.getInt(24) != AuditRingProtocol.RING_KIND_ACK) {
            throw new IOException("Existing ACK ring header/layout mismatch; refusing to reset it");
        }
        long producer = (long) LONG_VIEW.getAcquire(ring, AuditRingProtocol.PRODUCER_SEQUENCE_OFFSET);
        long consumer = (long) LONG_VIEW.getAcquire(ring, AuditRingProtocol.CONSUMER_SEQUENCE_OFFSET);
        if (Long.compareUnsigned(producer - consumer, AuditRingProtocol.ACK_CAPACITY) > 0) {
            throw new IOException("Existing ACK ring counters exceed capacity");
        }
    }

    private static int slotOffset(long sequence) {
        long index = sequence & (AuditRingProtocol.ACK_CAPACITY - 1L);
        return Math.toIntExact(AuditRingProtocol.SLOTS_OFFSET
                + index * AuditRingProtocol.ACK_SLOT_SIZE);
    }

    private static int crc32c(ByteBuffer source, int offset, int length) {
        ByteBuffer view = source.duplicate();
        view.position(offset).limit(offset + length);
        CRC32C crc = new CRC32C();
        crc.update(view.slice());
        return (int) crc.getValue();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
