package org.g12.audit;

import org.g12.audit.ipc.MappedAckRingWriter;
import org.g12.audit.ipc.MappedJournalReader;
import org.g12.audit.repository.AuditRepository;
import org.g12.audit.service.JournalConsumer;

import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class AuditApplication {
    private static final Logger LOGGER = Logger.getLogger(AuditApplication.class.getName());

    private AuditApplication() {
    }

    public static void main(String[] args) throws Exception {
        Path journalPath = Path.of(env("G12_JOURNAL_RING_PATH", "/dev/shm/g12-journal.ring"));
        Path ackPath = Path.of(env("G12_ACK_RING_PATH", "/dev/shm/g12-ack.ring"));
        String jdbcUrl = env("G12_DB_URL", "jdbc:postgresql://postgres:5432/g12audit");
        String dbUser = env("G12_DB_USER", "g12");
        String dbPassword = env("G12_DB_PASSWORD", "g12_dev_password");

        AuditRepository repository = new AuditRepository(jdbcUrl, dbUser, dbPassword);
        repository.initialize();
        repository.verifyConnection();
        LOGGER.info("PostgreSQL is ready; audit table is initialized.");

        try (MappedJournalReader journalReader = new MappedJournalReader(journalPath);
             MappedAckRingWriter ackWriter = new MappedAckRingWriter(ackPath)) {
            JournalConsumer consumer = new JournalConsumer(journalReader, ackWriter, repository);
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::stop, "g12-audit-shutdown"));
            LOGGER.info("Journal ring: " + journalPath + "; ACK ring: " + ackPath
                    + "; streamId=" + journalReader.streamId());
            consumer.run();
        } catch (Throwable failure) {
            LOGGER.log(Level.SEVERE, "Audit Service terminated", failure);
            throw failure;
        }
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
