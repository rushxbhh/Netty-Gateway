package org.java;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32C;

/** Single consumer for the Audit -> Gateway ACK ring. */
public final class MappedAckRingReader implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(MappedAckRingReader.class.getName());
    private static final VarHandle LONG_VIEW = MethodHandles.byteBufferViewVarHandle(
            long[].class, ByteOrder.LITTLE_ENDIAN);

    private final FileChannel channel;
    private final MappedByteBuffer ring;
    private final Function<AuditAcknowledgement, CompletableFuture<Void>> handler;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread readerThread;
    private volatile Throwable terminalFailure;

    public MappedAckRingReader(Path path,
            Function<AuditAcknowledgement, CompletableFuture<Void>> handler) throws IOException {
        this.handler = handler;
        if (ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN) {
            throw new IOException("G12 shared-memory protocol requires a little-endian host");
        }
        channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            FileLock lock = channel.lock();
            try {
                if (channel.size() < AckRingProtocol.FILE_SIZE) {
                    throw new IOException("ACK ring is missing or truncated: " + path);
                }
                ring = channel.map(FileChannel.MapMode.READ_WRITE, 0, AckRingProtocol.FILE_SIZE);
                ring.order(ByteOrder.LITTLE_ENDIAN);
                validateHeader();
                validateCounters();
            } finally {
                lock.release();
            }
        } catch (Throwable failure) {
            try { channel.close(); } catch (IOException closeFailure) { failure.addSuppressed(closeFailure); }
            if (failure instanceof IOException io) throw io;
            if (failure instanceof RuntimeException runtime) throw runtime;
            throw new IOException("Unable to map ACK ring", failure);
        }

        readerThread = Thread.ofPlatform().name("g12-ack-ring-reader").daemon(true)
                .unstarted(this::runLoop);
        readerThread.start();
    }

    public boolean isRunning() {
        return running.get() && readerThread.isAlive() && terminalFailure == null;
    }

    private void runLoop() {
        try {
            while (running.get() && !Thread.currentThread().isInterrupted()) {
                long consumer = (long) LONG_VIEW.get(ring, AckRingProtocol.CONSUMER_OFFSET);
                long producer = (long) LONG_VIEW.getAcquire(ring, AckRingProtocol.PRODUCER_OFFSET);
                long used = producer - consumer;
                if (Long.compareUnsigned(used, AckRingProtocol.CAPACITY) > 0) {
                    throw new IOException("ACK ring counters exceed capacity");
                }
                if (consumer == producer) {
                    LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
                    continue;
                }

                AuditAcknowledgement acknowledgement = readAndVerify(consumer);
                CompletableFuture<Void> delivery = handler.apply(acknowledgement);
                // Keep the ACK slot occupied until Gateway has routed the ACK to
                // the event loop and the WebSocket write has completed (or is dropped
                // because the original connection no longer exists).
                delivery.get();
                LONG_VIEW.setRelease(ring, AckRingProtocol.CONSUMER_OFFSET, consumer + 1);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (Throwable failure) {
            terminalFailure = failure;
            LOGGER.log(Level.SEVERE, "ACK ring reader stopped; consumerSeq was not advanced", failure);
        }
    }

    private AuditAcknowledgement readAndVerify(long sequence) throws IOException {
        int offset = slotOffset(sequence);
        long journalSequence = ring.getLong(offset);
        long transactionId = ring.getLong(offset + 8);
        long connectionId = ring.getLong(offset + 16);
        long auditedAt = ring.getLong(offset + 24);
        int auditStatus = ring.getInt(offset + 32);
        int journalStatus = ring.getInt(offset + 36);
        int storedChecksum = ring.getInt(offset + 60);
        int actualChecksum = crc32c(ring, offset, 60);

        if (storedChecksum != actualChecksum) {
            throw new IOException("ACK CRC32C mismatch at ACK ring sequence " + sequence);
        }
        if (auditStatus != 1 || (journalStatus != 0 && journalStatus != 1)) {
            throw new IOException("Unsupported ACK status at ACK ring sequence " + sequence);
        }
        return new AuditAcknowledgement(journalSequence, transactionId, connectionId,
                auditedAt, auditStatus, journalStatus, storedChecksum);
    }

    private void validateHeader() throws IOException {
        byte[] actual = new byte[8];
        for (int i = 0; i < actual.length; i++) actual[i] = ring.get(i);
        if (!Arrays.equals(actual, AckRingProtocol.MAGIC)
                || ring.getInt(8) != AckRingProtocol.VERSION
                || ring.getInt(12) != AckRingProtocol.SLOT_SIZE
                || ring.getLong(16) != AckRingProtocol.CAPACITY
                || ring.getInt(24) != AckRingProtocol.RING_KIND_ACK) {
            throw new IOException("ACK ring header/layout mismatch");
        }
    }

    private void validateCounters() throws IOException {
        long producer = (long) LONG_VIEW.getAcquire(ring, AckRingProtocol.PRODUCER_OFFSET);
        long consumer = (long) LONG_VIEW.getAcquire(ring, AckRingProtocol.CONSUMER_OFFSET);
        if (Long.compareUnsigned(producer - consumer, AckRingProtocol.CAPACITY) > 0) {
            throw new IOException("ACK ring counters exceed capacity");
        }
    }

    private static int slotOffset(long sequence) {
        long index = sequence & (AckRingProtocol.CAPACITY - 1L);
        return Math.toIntExact(AckRingProtocol.SLOTS_OFFSET + index * AckRingProtocol.SLOT_SIZE);
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
        running.set(false);
        readerThread.interrupt();
        try {
            readerThread.join(2_000);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while stopping ACK reader", interrupted);
        }
        if (readerThread.isAlive()) throw new IOException("ACK reader did not stop cleanly");
        channel.close();
        if (terminalFailure != null) {
            LOGGER.warning("ACK reader previously failed: " + terminalFailure);
        }
    }
}
