package org.g12.audit.service;

import org.g12.audit.ipc.MappedAckRingWriter;
import org.g12.audit.ipc.MappedJournalReader;
import org.g12.audit.model.AuditAcknowledgement;
import org.g12.audit.model.JournalRecord;
import org.g12.audit.repository.AuditRepository;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Ordered journal consumer. Persistence -> ACK publish -> journal consumerSeq.
 *
 * <p><b>Batching strategy:</b> On each iteration we drain as many journal slots as are
 * currently published (up to {@code MAX_BATCH_SIZE}) into a list, persist the entire
 * list in a single JDBC batch / one {@code COMMIT}, then emit ACKs and advance the
 * consumer counter for every record in the batch.  This amortises the cloud database
 * network round-trip across concurrent requests: N records per RTT instead of 1.
 *
 * <p>Ordering and idempotency guarantees are fully preserved:
 * <ul>
 *   <li>Records are read in strict journal-sequence order (SPSC consumer contract).
 *   <li>Each record's sequence ID is validated before it is accepted into the batch.
 *   <li>The batch uses {@code ON CONFLICT DO NOTHING} with per-row conflict verification.
 *   <li>Consumer counters are only advanced after persistence <em>and</em> ACK publication
 *       succeed for every record in the batch.
 * </ul>
 */
public final class JournalConsumer implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(JournalConsumer.class.getName());
    private static final long IDLE_PARK_NANOS = TimeUnit.MICROSECONDS.toNanos(100);

    /**
     * Maximum records to collect before committing a batch.
     * Sized to absorb typical concurrent bursts without delaying idle-path records.
     * Raising this reduces per-record commit cost but increases individual record latency
     * when the ring is busy — 32 is a good balance for ≤ 20 concurrent clients.
     */
    private static final int MAX_BATCH_SIZE = 32;

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
        LOGGER.info("Audit consumer started (batch mode, MAX_BATCH_SIZE=" + MAX_BATCH_SIZE
                + "). journal consumerSeq=" + journalReader.consumerSequence()
                + ", producerSeq=" + journalReader.producerSequence());

        final List<JournalRecord> batch = new ArrayList<>(MAX_BATCH_SIZE);

        while (running.get() && !Thread.currentThread().isInterrupted()) {

            // ── 1. Drain available records into the batch ─────────────────────
            batch.clear();
            long batchStartSequence = journalReader.consumerSequence();

            while (batch.size() < MAX_BATCH_SIZE) {
                long expectedSequence = batchStartSequence + batch.size();
                final JournalRecord record;
                try {
                    record = journalReader.readAt(expectedSequence);
                } catch (IOException integrityFailure) {
                    throw new IllegalStateException("Journal integrity/protocol failure", integrityFailure);
                }

                if (record == null) break; // No more records available right now.

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
                batch.add(record);
            }

            if (batch.isEmpty()) {
                LockSupport.parkNanos(IDLE_PARK_NANOS);
                continue;
            }

            // ── 2. Persist batch (one network round-trip for all N records) ───
            try {
                if (batch.size() == 1) {
                    // Single-record fast path: skip batch overhead.
                    repository.persistIdempotently(journalReader.streamId(), batch.get(0));
                } else {
                    repository.persistBatch(journalReader.streamId(), batch);
                    if (LOGGER.isLoggable(java.util.logging.Level.FINE)) {
                        LOGGER.fine("Persisted batch of " + batch.size()
                                + " records [seq " + batchStartSequence
                                + ".." + (batchStartSequence + batch.size() - 1) + "]");
                    }
                }
            } catch (SQLException databaseFailure) {
                LOGGER.log(Level.SEVERE,
                        "PostgreSQL persistence failed; batch will be retried at seq "
                                + batchStartSequence, databaseFailure);
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                continue; // consumerSeq unchanged → entire batch retried
            }

            // ── 3. ACK and advance consumer for each record in order ──────────
            for (JournalRecord record : batch) {
                try {
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
                    journalReader.advanceConsumer(record.sequenceId());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    running.set(false);
                    return;
                } catch (IOException ringFailure) {
                    throw new IllegalStateException("Unable to publish ACK or advance journal ring", ringFailure);
                }
            }
        }
        LOGGER.info("Audit consumer stopped.");
    }
}
