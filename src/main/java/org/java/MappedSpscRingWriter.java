package org.java;

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
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * Publishes validated FlatBuffers transactions into the C++ sequencer's inbound
 * memory-mapped SPSC ring.
 *
 * Netty can call submit() from multiple event-loop threads. Those calls are
 * staged in a bounded MPSC queue. A single dedicated worker is the only thread
 * that writes the actual SPSC ring, preserving the C++ ring's one-producer
 * contract.
 *
 * Ring layout must remain byte-for-byte compatible with:
 * sequencer/include/sequencer/ring_layout.hpp
 */
public final class MappedSpscRingWriter implements AutoCloseable {

    // ----- Shared C++ ring_layout.hpp contract -----

    private static final byte[] RING_MAGIC = RingProtocol.RING_MAGIC_BYTES;

    private static final int RING_VERSION = RingProtocol.RING_VERSION;
    private static final int RING_KIND_INBOUND = RingProtocol.RING_KIND_INBOUND;

    private static final int OFFSET_HEADER = RingProtocol.OFFSET_HEADER;
    private static final int OFFSET_PRODUCER_SEQUENCE = RingProtocol.OFFSET_PRODUCER_SEQUENCE;
    private static final int OFFSET_CONSUMER_SEQUENCE = RingProtocol.OFFSET_CONSUMER_SEQUENCE;
    private static final int OFFSET_SLOTS = RingProtocol.OFFSET_SLOTS;

    private static final int INBOUND_METADATA_SIZE = RingProtocol.INBOUND_METADATA_SIZE;
    private static final int INBOUND_SLOT_SIZE = RingProtocol.INBOUND_SLOT_SIZE;
    private static final int INBOUND_CAPACITY = RingProtocol.INBOUND_CAPACITY;
    private static final int MAX_PAYLOAD_SIZE = RingProtocol.INBOUND_PAYLOAD_MAX;

    private static final long SLOT_MASK = INBOUND_CAPACITY - 1L;
    private static final long FILE_SIZE = RingProtocol.INBOUND_FILE_SIZE;

    /**
     * ByteBuffer-view VarHandle whose indexes are byte offsets, not long indexes.
     * Offsets 64 and 128 are 8-byte aligned, as required for atomic 64-bit access.
     */
    private static final VarHandle LONG_VIEW =
            MethodHandles.byteBufferViewVarHandle(
                    long[].class,
                    ByteOrder.LITTLE_ENDIAN
            );

    private static final int DEFAULT_QUEUE_CAPACITY = 4_096;
    private static final long FULL_RING_RETRY_NANOS = 50_000L;
    private static final long FULL_RING_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(2);

    public enum Status {
        /** The slot was written and producerSeq was published. */
        PUBLISHED,

        /** The gateway's bounded MPSC staging queue had no free entry. */
        QUEUE_FULL,

        /** C++ did not free an inbound slot before the ring-full timeout. */
        RING_FULL,

        /** The writer is shutting down or is no longer accepting requests. */
        SHUTTING_DOWN
    }

    /**
     * inboundSequence is the sequence number of the inbound ring slot. It is
     * not the C++ journal sequence number assigned later by the sequencer.
     */
    public record PublishResult(Status status, long inboundSequence) {

        public boolean published() {
            return status == Status.PUBLISHED;
        }

        private static PublishResult published(long sequence) {
            return new PublishResult(Status.PUBLISHED, sequence);
        }

        private static PublishResult rejected(Status status) {
            return new PublishResult(status, -1L);
        }
    }

    private record PendingWrite(
            byte[] payload,
            long connectionId,
            long transactionId,
            CompletableFuture<PublishResult> completion
    ) {}

    private record PublishAttempt(boolean published, long sequence) {
        private static PublishAttempt full() {
            return new PublishAttempt(false, -1L);
        }

        private static PublishAttempt published(long sequence) {
            return new PublishAttempt(true, sequence);
        }
    }

    private final FileChannel channel;
    private final ArrayBlockingQueue<PendingWrite> pendingWrites;
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final Object lifecycleLock = new Object();
    private final Thread writerThread;

    // Accessed by the writer thread after construction; volatile for safe close visibility.
    private volatile MappedByteBuffer mappedBuffer;
    private volatile Throwable terminalFailure;

    public MappedSpscRingWriter(Path ringPath) throws IOException {
        this(ringPath, DEFAULT_QUEUE_CAPACITY);
    }

    public MappedSpscRingWriter(Path ringPath, int queueCapacity) throws IOException {
        Objects.requireNonNull(ringPath, "ringPath");

        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be positive");
        }

        // ring_layout.hpp stores native C++ integers. This protocol currently
        // targets little-endian Linux deployments (x86_64 and ARM64).
        if (ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN) {
            throw new IOException(
                    "The G12 ring protocol currently requires a little-endian host"
            );
        }

        Path absolutePath = ringPath.toAbsolutePath().normalize();
        Path parent = absolutePath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        FileChannel openedChannel = FileChannel.open(
                absolutePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );

        MappedByteBuffer mapping;
        try {
            // Serialize initialization if two Java gateway instances accidentally
            // start against the same new file. This is a startup lock, not a hot-path lock.
            FileLock startupLock = openedChannel.lock();
            try {
                long currentSize = openedChannel.size();
                boolean newFile = currentSize == 0;

                if (newFile) {
                    extendFile(openedChannel, FILE_SIZE);
                } else if (currentSize < FILE_SIZE) {
                    throw new IOException(
                            "Inbound ring file is too small: expected at least "
                                    + FILE_SIZE + " bytes, found " + currentSize
                    );
                }

                mapping = openedChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        0,
                        FILE_SIZE
                );
                mapping.order(ByteOrder.LITTLE_ENDIAN);

                if (newFile) {
                    initializeNewRing(mapping);
                } else {
                    validateExistingRing(mapping);
                }
            } finally {
                startupLock.release();
            }
        } catch (Throwable failure) {
            try {
                openedChannel.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }

            if (failure instanceof IOException ioException) {
                throw ioException;
            }
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException("Unable to initialize inbound ring", failure);
        }

        this.channel = openedChannel;
        this.mappedBuffer = mapping;
        this.pendingWrites = new ArrayBlockingQueue<>(queueCapacity);

        this.writerThread = Thread.ofPlatform()
                .name("g12-inbound-ring-writer")
                .daemon(true)
                .unstarted(this::runWriterLoop);
        this.writerThread.start();
    }

    /**
     * Submit a validated transaction from any Netty event-loop thread.
     *
     * The payload is copied before this method returns because Netty releases
     * inbound ByteBuf instances after channelRead0() completes.
     *
     * The returned future completes only after the dedicated writer has copied
     * the slot and release-published producerSeq. A successful result therefore
     * means "accepted by the inbound ring", not "sequenced" or "audited".
     */
    public CompletableFuture<PublishResult> submit(
            byte[] payload,
            long connectionId,
            long transactionId
    ) {
        Objects.requireNonNull(payload, "payload");

        if (payload.length == 0 || payload.length > MAX_PAYLOAD_SIZE) {
            throw new IllegalArgumentException(
                    "payload length must be between 1 and " + MAX_PAYLOAD_SIZE
            );
        }
        if (connectionId <= 0) {
            throw new IllegalArgumentException("connectionId must be positive");
        }
        if (transactionId <= 0) {
            throw new IllegalArgumentException("transactionId must be positive");
        }

        CompletableFuture<PublishResult> completion = new CompletableFuture<>();
        byte[] ownedPayload = Arrays.copyOf(payload, payload.length);
        PendingWrite pending = new PendingWrite(
                ownedPayload,
                connectionId,
                transactionId,
                completion
        );

        synchronized (lifecycleLock) {
            if (!accepting.get()) {
                completion.complete(PublishResult.rejected(Status.SHUTTING_DOWN));
                return completion;
            }

            // Never block a Netty event loop. Saturation becomes an explicit
            // overload response instead of stalling network I/O.
            if (!pendingWrites.offer(pending)) {
                completion.complete(PublishResult.rejected(Status.QUEUE_FULL));
            }
        }

        return completion;
    }

    /** Current inbound producer sequence, read with acquire semantics. */
    public long producerSequence() {
        return (long) LONG_VIEW.getAcquire(
                requireMapping(),
                OFFSET_PRODUCER_SEQUENCE
        );
    }

    /** Current C++ consumer sequence, read with acquire semantics. */
    public long consumerSequence() {
        return (long) LONG_VIEW.getAcquire(
                requireMapping(),
                OFFSET_CONSUMER_SEQUENCE
        );
    }

    public int queuedWrites() {
        return pendingWrites.size();
    }

    public boolean isAccepting() {
        return accepting.get();
    }

    private void runWriterLoop() {
        PendingWrite current = null;

        try {
            while (accepting.get()) {
                try {
                    current = pendingWrites.poll(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException interrupted) {
                    if (!accepting.get()) {
                        break;
                    }
                    continue;
                }

                if (current == null) {
                    continue;
                }

                boolean completed = false;
                long ringFullSince = 0L;

                // If the SPSC ring is full, wait for C++ to advance consumerSeq.
                // Only this worker retries; the Netty event loops remain non-blocking.
                while (accepting.get()) {
                    PublishAttempt attempt = publishOne(current);

                    if (attempt.published()) {
                        current.completion().complete(
                                PublishResult.published(attempt.sequence())
                        );
                        completed = true;
                        break;
                    }

                    long now = System.nanoTime();
                    if (ringFullSince == 0L) {
                        ringFullSince = now;
                    } else if (now - ringFullSince >= FULL_RING_TIMEOUT_NANOS) {
                        current.completion().complete(
                                PublishResult.rejected(Status.RING_FULL)
                        );
                        completed = true;
                        break;
                    }

                    LockSupport.parkNanos(FULL_RING_RETRY_NANOS);
                    if (Thread.currentThread().isInterrupted() && !accepting.get()) {
                        break;
                    }
                }

                if (!completed) {
                    current.completion().complete(
                            PublishResult.rejected(Status.SHUTTING_DOWN)
                    );
                }
                current = null;
            }
        } catch (Throwable failure) {
            terminalFailure = failure;
            accepting.set(false);

            if (current != null) {
                current.completion().completeExceptionally(failure);
            }
        } finally {
            PendingWrite abandoned;
            while ((abandoned = pendingWrites.poll()) != null) {
                if (terminalFailure != null) {
                    abandoned.completion().completeExceptionally(terminalFailure);
                } else {
                    abandoned.completion().complete(
                            PublishResult.rejected(Status.SHUTTING_DOWN)
                    );
                }
            }
        }
    }

    /**
     * Called only by writerThread: this is the sole producer of the SPSC ring.
     */
    private PublishAttempt publishOne(PendingWrite pending) {
        MappedByteBuffer ring = requireMapping();

        // We own producerSeq, so a plain load is sufficient. consumerSeq is
        // written by C++ and must be read with acquire before slot reuse.
        long producer = (long) LONG_VIEW.get(
                ring,
                OFFSET_PRODUCER_SEQUENCE
        );
        long consumer = (long) LONG_VIEW.getAcquire(
                ring,
                OFFSET_CONSUMER_SEQUENCE
        );

        // Unsigned subtraction preserves the uint64_t ring-counter protocol.
        long used = producer - consumer;
        int occupancyComparison = Long.compareUnsigned(used, INBOUND_CAPACITY);

        if (occupancyComparison == 0) {
            return PublishAttempt.full();
        }
        if (occupancyComparison > 0) {
            throw new IllegalStateException(
                    "Inbound ring counters are inconsistent: producer="
                            + Long.toUnsignedString(producer)
                            + ", consumer=" + Long.toUnsignedString(consumer)
            );
        }

        int slotOffset = (int) (
                OFFSET_SLOTS + (producer & SLOT_MASK) * INBOUND_SLOT_SIZE
        );

        // Inbound slot layout from ring_layout.hpp:
        // 0..31 metadata (payloadLen, flags, connectionId, transactionId, reserved)
        // 32..127 are the 96-byte FlatBuffer payload area.
        ring.putInt(slotOffset, pending.payload().length);
        ring.putInt(slotOffset + 4, 0);
        ring.putLong(slotOffset + 8, pending.connectionId());
        ring.putLong(slotOffset + RingProtocol.INBOUND_CLIENT_TXN_ID_OFFSET,
                pending.transactionId());

        for (int i = 24; i < INBOUND_METADATA_SIZE; i++) {
            ring.put(slotOffset + i, (byte) 0);
        }

        ByteBuffer payloadView = ring.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        payloadView.position(slotOffset + 32);
        payloadView.put(pending.payload());

        // Clear the unused payload tail so slot reuse never exposes stale bytes.
        for (int i = pending.payload().length; i < MAX_PAYLOAD_SIZE; i++) {
            ring.put(slotOffset + 32 + i, (byte) 0);
        }

        // Publish last. C++ loads producerSeq with memory_order_acquire before
        // reading the slot, so it must not observe a partially written record.
        LONG_VIEW.setRelease(
                ring,
                OFFSET_PRODUCER_SEQUENCE,
                producer + 1
        );

        return PublishAttempt.published(producer);
    }

    private MappedByteBuffer requireMapping() {
        MappedByteBuffer mapping = mappedBuffer;
        if (mapping == null) {
            throw new IllegalStateException("Inbound ring mapping is closed");
        }
        return mapping;
    }

    private static void extendFile(FileChannel channel, long requiredSize)
            throws IOException {
        if (channel.size() >= requiredSize) {
            return;
        }

        channel.position(requiredSize - 1);
        channel.write(ByteBuffer.wrap(new byte[]{0}));
    }

    private static void initializeNewRing(MappedByteBuffer ring) {
        // Initialize only the header and the two sequence cache lines.
        // The OS-created file is zero-filled; the slot area need not be swept.
        for (int i = 0; i < OFFSET_SLOTS; i++) {
            ring.put(i, (byte) 0);
        }

        ring.putInt(8, RING_VERSION);
        ring.putInt(12, INBOUND_SLOT_SIZE);
        ring.putLong(16, INBOUND_CAPACITY);
        ring.putInt(24, RING_KIND_INBOUND);

        LONG_VIEW.setRelease(ring, OFFSET_PRODUCER_SEQUENCE, 0L);
        LONG_VIEW.setRelease(ring, OFFSET_CONSUMER_SEQUENCE, 0L);

        // Header magic is the readiness marker and is written last. The
        // cross-process startup file lock ensures only one process initializes
        // an empty ring at a time.
        VarHandle.releaseFence();
        for (int i = 0; i < RING_MAGIC.length; i++) {
            ring.put(OFFSET_HEADER + i, RING_MAGIC[i]);
        }
    }

    private static void validateExistingRing(MappedByteBuffer ring)
            throws IOException {
        byte[] actualMagic = new byte[RING_MAGIC.length];
        for (int i = 0; i < actualMagic.length; i++) {
            actualMagic[i] = ring.get(OFFSET_HEADER + i);
        }

        if (!Arrays.equals(actualMagic, RING_MAGIC)) {
            throw new IOException(
                    "Existing inbound ring has an invalid magic header; refusing to reset it"
            );
        }

        int version = ring.getInt(8);
        int slotSize = ring.getInt(12);
        long capacity = ring.getLong(16);
        int ringKind = ring.getInt(24);

        if (version != RING_VERSION
                || slotSize != INBOUND_SLOT_SIZE
                || capacity != INBOUND_CAPACITY
                || ringKind != RING_KIND_INBOUND) {
            throw new IOException(
                    "Existing inbound ring layout mismatch: version=" + version
                            + ", slotSize=" + slotSize
                            + ", capacity=" + capacity
                            + ", ringKind=" + ringKind
            );
        }

        long producer = (long) LONG_VIEW.getAcquire(
                ring,
                OFFSET_PRODUCER_SEQUENCE
        );
        long consumer = (long) LONG_VIEW.getAcquire(
                ring,
                OFFSET_CONSUMER_SEQUENCE
        );
        long used = producer - consumer;

        if (Long.compareUnsigned(used, INBOUND_CAPACITY) > 0) {
            throw new IOException(
                    "Existing inbound ring counters are inconsistent: producer="
                            + Long.toUnsignedString(producer)
                            + ", consumer=" + Long.toUnsignedString(consumer)
            );
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (lifecycleLock) {
            if (!accepting.getAndSet(false)) {
                // Continue below so channel closure remains idempotent.
            }
        }

        writerThread.interrupt();
        try {
            writerThread.join(2_000);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while stopping inbound ring writer", interrupted);
        }

        if (writerThread.isAlive()) {
            throw new IOException("Inbound ring writer thread did not stop cleanly");
        }

        mappedBuffer = null;
        channel.close();
    }
}
