package org.g12.audit.service;

import org.g12.audit.ipc.MappedAckRingWriter;
import org.g12.audit.ipc.MappedJournalReader;
import org.g12.audit.model.AuditAcknowledgement;
import org.g12.audit.model.JournalRecord;
import org.g12.audit.repository.AuditRepository;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Ordered journal consumer. Persistence -> ACK publish -> journal consumerSeq. */
public final class JournalConsumer implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(JournalConsumer.class.getName());
    private static final long IDLE_PARK_NANOS = TimeUnit.MICROSECONDS.toNanos(100);

    private final MappedJournalReader journalReader;
    private final MappedAckRingWriter ackWriter;
    private final AuditRepository repository;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public JournalConsumer(MappedJournalReader journalReader,
                           MappedAckRingWriter ackWriter,
                           AuditRepository repository) {
        this.journalReader = journalReader;
        this.ackWriter = ackWriter;
        this.repository = repository;
    }

    public void stop() {
        running.set(false);
    }

    @Override
    public void run() {
        LOGGER.info("Audit consumer started. journal consumerSeq="
                + journalReader.consumerSequence() + ", producerSeq="
                + journalReader.producerSequence());

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            final long expectedSequence = journalReader.consumerSequence();
            final JournalRecord record;
            try {
                record = journalReader.readNext();
            } catch (IOException integrityFailure) {
                // Do not skip corrupt records. Leave consumerSeq unchanged and fail visibly.
                throw new IllegalStateException("Journal integrity/protocol failure", integrityFailure);
            }

            if (record == null) {
                LockSupport.parkNanos(IDLE_PARK_NANOS);
                continue;
            }

            if (record.sequenceId() != expectedSequence) {
                throw new IllegalStateException("Journal ordering failure: expected sequence "
                        + expectedSequence + ", received " + record.sequenceId());
            }
            if (record.connectionId() <= 0) {
                throw new IllegalStateException("Journal record has invalid connectionId at sequence "
                        + record.sequenceId());
            }
            if (record.status() != 0 && record.status() != 1) {
                throw new IllegalStateException("Unknown journal status " + record.status()
                        + " at sequence " + record.sequenceId());
            }

            try {
                // 1. Durable idempotent persistence.
                repository.persistIdempotently(journalReader.streamId(), record);

                // 2. Publish the final acknowledgement. If ACK ring is full, wait;
                //    journal consumerSeq must remain unchanged to apply backpressure.
                AuditAcknowledgement acknowledgement = new AuditAcknowledgement(
                        record.sequenceId(),
                        record.clientTxnId(),
                        record.connectionId(),
                        System.currentTimeMillis(),
                        1,
                        record.status(),
                        0
                );
                ackWriter.publish(acknowledgement, running::get);

                // 3. Release the journal slot only after the ACK is published.
                journalReader.advanceConsumer(expectedSequence);

            } catch (SQLException databaseFailure) {
                LOGGER.log(Level.SEVERE,
                        "PostgreSQL persistence failed; journal slot will be retried: "
                                + record.sequenceId(), databaseFailure);
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                running.set(false);
            } catch (IOException ringFailure) {
                throw new IllegalStateException("Unable to publish ACK or advance journal ring", ringFailure);
            }
        }
        LOGGER.info("Audit consumer stopped.");
    }
}
