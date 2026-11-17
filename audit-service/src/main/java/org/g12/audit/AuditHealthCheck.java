package org.g12.audit;

import org.g12.audit.ipc.AuditRingProtocol;
import org.g12.audit.repository.AuditRepository;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/** Container health check: ACK ring ABI is present and PostgreSQL accepts queries. */
public final class AuditHealthCheck {
    private AuditHealthCheck() {
    }

    public static void main(String[] args) throws Exception {
        Path ackPath = Path.of(env("G12_ACK_RING_PATH", "/dev/shm/g12-ack.ring"));
        try (FileChannel channel = FileChannel.open(ackPath, StandardOpenOption.READ)) {
            if (channel.size() < AuditRingProtocol.ACK_FILE_SIZE) {
                throw new IllegalStateException("ACK ring is missing or truncated");
            }
            ByteBuffer header = channel.map(FileChannel.MapMode.READ_ONLY, 0, 64)
                    .order(ByteOrder.LITTLE_ENDIAN);
            byte[] magic = new byte[8];
            for (int i = 0; i < magic.length; i++) magic[i] = header.get(i);
            if (!Arrays.equals(magic, AuditRingProtocol.MAGIC)
                    || header.getInt(8) != AuditRingProtocol.VERSION
                    || header.getInt(12) != AuditRingProtocol.ACK_SLOT_SIZE
                    || header.getLong(16) != AuditRingProtocol.ACK_CAPACITY
                    || header.getInt(24) != AuditRingProtocol.RING_KIND_ACK) {
                throw new IllegalStateException("ACK ring header mismatch");
            }
        }
        new AuditRepository(
                env("G12_DB_URL", "jdbc:postgresql://postgres:5432/g12audit"),
                env("G12_DB_USER", "g12"),
                env("G12_DB_PASSWORD", "g12_dev_password")
        ).verifyConnection();
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
