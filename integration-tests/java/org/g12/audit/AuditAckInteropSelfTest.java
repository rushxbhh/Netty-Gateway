package org.g12.audit;

import org.g12.audit.ipc.MappedAckRingWriter;
import org.g12.audit.model.AuditAcknowledgement;
import org.java.MappedAckRingReader;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Tests the actual Audit producer and Gateway consumer implementations against one mapped ACK file. */
public final class AuditAckInteropSelfTest {
    private static final VarHandle LONG_VIEW = MethodHandles.byteBufferViewVarHandle(
            long[].class, ByteOrder.LITTLE_ENDIAN);

    private AuditAckInteropSelfTest() {
    }

    public static void main(String[] args) throws Exception {
        Path ackPath = Files.createTempFile("g12-ack-interop-", ".ring");
        Files.delete(ackPath);
        AtomicReference<org.java.AuditAcknowledgement> received = new AtomicReference<>();
        AuditAcknowledgement expected = new AuditAcknowledgement(
                9, 9001, 77, 1_800_000_000_000L, 1, 0, 0);

        try {
            try (MappedAckRingWriter writer = new MappedAckRingWriter(ackPath)) {
                writer.publish(expected, () -> true);
                try (MappedAckRingReader reader = new MappedAckRingReader(ackPath, actual -> {
                    received.set(actual);
                    if (actual.journalSequence() != expected.journalSequence()
                            || actual.transactionId() != expected.transactionId()
                            || actual.connectionId() != expected.connectionId()
                            || actual.auditStatus() != expected.auditStatus()
                            || actual.journalStatus() != expected.journalStatus()) {
                        return CompletableFuture.failedFuture(
                                new AssertionError("Gateway decoded ACK fields incorrectly: " + actual));
                    }
                    return CompletableFuture.completedFuture(null);
                })) {
                    check(reader.isRunning(), "Gateway ACK reader thread must start");
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                    while (readConsumer(ackPath) != 1 && System.nanoTime() < deadline) {
                        Thread.sleep(5);
                    }
                    check(readConsumer(ackPath) == 1,
                            "Gateway must advance ACK consumerSeq after routing ACK");
                }
            }

            check(received.get() != null, "Gateway ACK reader must receive a record");
            check(received.get().toWireText().contains("stage=AUDITED"),
                    "decoded acknowledgement must represent final audit status");
            System.out.println("PASS: Audit ACK writer -> Gateway ACK reader shared-memory interoperability");
        } finally {
            Files.deleteIfExists(ackPath);
        }
    }

    private static long readConsumer(Path path) throws Exception {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            MappedByteBuffer ring = channel.map(FileChannel.MapMode.READ_ONLY, 0, 4_194_496);
            return (long) LONG_VIEW.getAcquire(ring, 128);
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
