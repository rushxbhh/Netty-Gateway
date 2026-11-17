package org.g12.audit;

import org.g12.audit.ipc.AuditRingProtocol;
import org.g12.audit.ipc.MappedAckRingWriter;
import org.g12.audit.ipc.MappedJournalReader;
import org.g12.audit.model.AuditAcknowledgement;
import org.g12.audit.model.JournalRecord;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;

/** Dependency-free smoke test for journal reading, CRC checking and ACK publication. */
public final class AuditProtocolSelfTest {
    private static final VarHandle LONG_VIEW = MethodHandles.byteBufferViewVarHandle(
            long[].class, ByteOrder.LITTLE_ENDIAN);

    private AuditProtocolSelfTest() {
    }

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("g12-audit-selftest-");
        Path journalPath = dir.resolve("journal.ring");
        Path ackPath = dir.resolve("ack.ring");
        try {
            createJournalWithOneRecord(journalPath);
            try (MappedJournalReader journal = new MappedJournalReader(journalPath);
                 MappedAckRingWriter acknowledgements = new MappedAckRingWriter(ackPath)) {
                JournalRecord record = journal.readNext();
                check(record != null, "journal record must be available");
                check(record.sequenceId() == 0, "journal sequence");
                check(record.clientTxnId() == 9001, "transaction id");
                check(record.connectionId() == 77, "connection id");
                check(record.amount() == 12345, "amount");
                check(journal.streamId() != 0, "journal stream ID initialized");
                long streamId = journal.streamId();

                acknowledgements.publish(new AuditAcknowledgement(
                        record.sequenceId(), record.clientTxnId(), record.connectionId(),
                        1_800_000_000_000L, AuditRingProtocol.AUDIT_STATUS_PERSISTED,
                        record.status(), 0), () -> true);

                check(acknowledgements.producerSequence() == 1, "ACK producerSeq publication");
                check(acknowledgements.consumerSequence() == 0, "ACK consumerSeq remains owned by Gateway");

                journal.advanceConsumer(0);
                check(journal.consumerSequence() == 1, "journal consumerSeq advancement");
                check(journal.readNext() == null, "journal should now be empty");
            }
            verifyAckHeaderAndRecord(ackPath);
            try (MappedJournalReader reopened = new MappedJournalReader(journalPath)) {
                check(reopened.streamId() != 0, "journal stream ID survives reader restart");
            }
            System.out.println("PASS: Audit protocol self-test (journal map, CRC32C, ACK publish, counters, stream ID)");
        } finally {
            Files.deleteIfExists(ackPath);
            Files.deleteIfExists(journalPath);
            Files.deleteIfExists(dir);
        }
    }

    private static void createJournalWithOneRecord(Path path) throws Exception {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            channel.position(AuditRingProtocol.JOURNAL_FILE_SIZE - 1);
            channel.write(ByteBuffer.wrap(new byte[]{0}));
            java.nio.MappedByteBuffer ring = channel.map(FileChannel.MapMode.READ_WRITE, 0,
                    AuditRingProtocol.JOURNAL_FILE_SIZE);
            ring.order(ByteOrder.LITTLE_ENDIAN);
            writeHeader(ring, AuditRingProtocol.JOURNAL_SLOT_SIZE,
                    AuditRingProtocol.JOURNAL_CAPACITY, AuditRingProtocol.RING_KIND_JOURNAL);
            LONG_VIEW.setRelease(ring, AuditRingProtocol.PRODUCER_SEQUENCE_OFFSET, 1L);
            LONG_VIEW.setRelease(ring, AuditRingProtocol.CONSUMER_SEQUENCE_OFFSET, 0L);

            int slot = AuditRingProtocol.SLOTS_OFFSET;
            ring.putLong(slot, 0L);
            ring.putLong(slot + 8, 123456789L);
            ring.putLong(slot + 16, 9001L);
            ring.putLong(slot + 24, 77L);
            ring.putLong(slot + 32, 101L);
            ring.putLong(slot + 40, 202L);
            ring.putLong(slot + 48, 12345L);
            ring.put(slot + 56, (byte) 0);
            ring.put(slot + 57, (byte) 0);
            ring.putShort(slot + 58, (short) 0);
            ring.putInt(slot + 60, crc32c(ring, slot, 60));
            ring.force();
        }
    }

    private static void writeHeader(ByteBuffer ring, int slotSize, long capacity, int kind) {
        for (int i = 0; i < AuditRingProtocol.SLOTS_OFFSET; i++) ring.put(i, (byte) 0);
        ring.putInt(8, AuditRingProtocol.VERSION);
        ring.putInt(12, slotSize);
        ring.putLong(16, capacity);
        ring.putInt(24, kind);
        for (int i = 0; i < AuditRingProtocol.MAGIC.length; i++) ring.put(i, AuditRingProtocol.MAGIC[i]);
    }

    private static void verifyAckHeaderAndRecord(Path path) throws Exception {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer ring = channel.map(FileChannel.MapMode.READ_ONLY, 0,
                    AuditRingProtocol.ACK_FILE_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            check(ring.getInt(8) == AuditRingProtocol.VERSION, "ACK version");
            check(ring.getInt(12) == AuditRingProtocol.ACK_SLOT_SIZE, "ACK slot size");
            check(ring.getLong(16) == AuditRingProtocol.ACK_CAPACITY, "ACK capacity");
            check(ring.getInt(24) == AuditRingProtocol.RING_KIND_ACK, "ACK kind");
            check((long) LONG_VIEW.getAcquire(ring, AuditRingProtocol.PRODUCER_SEQUENCE_OFFSET) == 1,
                    "ACK producer counter");
            int slot = AuditRingProtocol.SLOTS_OFFSET;
            check(ring.getLong(slot) == 0, "ACK journal sequence");
            check(ring.getLong(slot + 8) == 9001, "ACK transaction ID");
            check(ring.getLong(slot + 16) == 77, "ACK connection ID");
            check(ring.getInt(slot + 32) == AuditRingProtocol.AUDIT_STATUS_PERSISTED, "ACK status");
            check(ring.getInt(slot + 60) == crc32c(ring, slot, 60), "ACK CRC32C");
        }
    }

    private static int crc32c(ByteBuffer source, int offset, int length) {
        ByteBuffer view = source.duplicate();
        view.position(offset).limit(offset + length);
        CRC32C crc = new CRC32C();
        crc.update(view.slice());
        return (int) crc.getValue();
    }

    private static void check(boolean condition, String name) {
        if (!condition) throw new AssertionError("Failed: " + name);
    }
}
