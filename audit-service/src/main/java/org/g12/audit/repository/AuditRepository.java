package org.g12.audit.repository;

import org.g12.audit.model.JournalRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/** PostgreSQL persistence for immutable sequencer journal records. */
public final class AuditRepository implements AutoCloseable {
    private static final String CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS transaction_audit (
                stream_id BIGINT NOT NULL,
                sequence_id BIGINT NOT NULL,
                client_txn_id BIGINT NOT NULL,
                connection_id BIGINT NOT NULL,
                from_account BIGINT NOT NULL,
                to_account BIGINT NOT NULL,
                amount BIGINT NOT NULL,
                transaction_type SMALLINT NOT NULL,
                journal_status SMALLINT NOT NULL,
                timestamp_tsc BIGINT NOT NULL,
                checksum BIGINT NOT NULL,
                audited_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (stream_id, sequence_id)
            )
            """;

    private static final String INSERT = """
            INSERT INTO transaction_audit (
                stream_id, sequence_id, client_txn_id, connection_id,
                from_account, to_account, amount,
                transaction_type, journal_status, timestamp_tsc, checksum
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (stream_id, sequence_id) DO NOTHING
            """;

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private Connection persistentConnection;

    public AuditRepository(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    private synchronized Connection getOrCreateConnection() throws SQLException {
        if (persistentConnection != null && !persistentConnection.isClosed()) {
            return persistentConnection;
        }
        if (persistentConnection != null) {
            try { persistentConnection.close(); } catch (Exception ignored) {}
        }
        persistentConnection = connect();
        persistentConnection.setAutoCommit(false);
        return persistentConnection;
    }

    public void initialize() throws SQLException {
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement(CREATE_TABLE)) {
            statement.execute();
        }
    }

    /**
     * Single-record idempotent insert. Kept for unit tests and sequential-load paths.
     * Under concurrent load, prefer {@link #persistBatch(long, List)}.
     */
    public synchronized void persistIdempotently(long streamId, JournalRecord record) throws SQLException {
        Connection connection = getOrCreateConnection();
        try {
            int inserted;
            try (PreparedStatement statement = connection.prepareStatement(INSERT)) {
                bindRecord(statement, streamId, record);
                inserted = statement.executeUpdate();
            }
            if (inserted == 0) {
                verifyExisting(connection, streamId, record);
            }
            connection.commit();
        } catch (SQLException | RuntimeException failure) {
            rollbackAndInvalidate(connection, failure);
            throw failure;
        }
    }

    /**
     * Batch idempotent insert: sends all records in a single JDBC batch, then commits once.
     * <p>
     * This amortises the cloud database network round-trip across N records,
     * reducing per-record commit latency from O(N × RTT) to O(1 × RTT).
     * Sequence ordering is preserved by the caller (JournalConsumer drains in journal order).
     * Any conflict on an individual row triggers a per-row verification; the whole batch
     * still commits if all conflicts are benign replays.
     * On any failure the connection is rolled back and invalidated so the next call
     * reconnects cleanly.
     *
     * @param streamId the volatile ring stream identifier
     * @param records  ordered list of records to persist; must not be empty
     * @throws SQLException if persistence or verification fails
     */
    public synchronized void persistBatch(long streamId, List<JournalRecord> records) throws SQLException {
        if (records.isEmpty()) return;
        Connection connection = getOrCreateConnection();
        try (PreparedStatement statement = connection.prepareStatement(INSERT)) {
            for (JournalRecord record : records) {
                bindRecord(statement, streamId, record);
                statement.addBatch();
            }
            int[] results = statement.executeBatch();
            // Verify any rows that conflicted (DO NOTHING → count 0).
            for (int i = 0; i < results.length; i++) {
                if (results[i] == 0) {
                    verifyExisting(connection, streamId, records.get(i));
                }
            }
            connection.commit();
        } catch (SQLException | RuntimeException failure) {
            rollbackAndInvalidate(connection, failure);
            throw failure;
        }
    }

    @Override
    public synchronized void close() {
        if (persistentConnection != null) {
            try { persistentConnection.close(); } catch (Exception ignored) {}
            persistentConnection = null;
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static void bindRecord(PreparedStatement statement, long streamId, JournalRecord record)
            throws SQLException {
        statement.setLong(1, streamId);
        statement.setLong(2, record.sequenceId());
        statement.setLong(3, record.clientTxnId());
        statement.setLong(4, record.connectionId());
        statement.setLong(5, record.fromAccount());
        statement.setLong(6, record.toAccount());
        statement.setLong(7, record.amount());
        statement.setShort(8, (short) record.type());
        statement.setShort(9, (short) record.status());
        statement.setLong(10, record.timestampTsc());
        statement.setLong(11, Integer.toUnsignedLong(record.checksum()));
    }

    private void rollbackAndInvalidate(Connection connection, Exception cause) {
        try { connection.rollback(); } catch (SQLException rollbackFailure) {
            cause.addSuppressed(rollbackFailure);
        }
        try { connection.close(); } catch (Exception ignored) {}
        persistentConnection = null;
    }

    private static void verifyExisting(Connection connection, long streamId, JournalRecord record)
            throws SQLException {
        String sql = """
                SELECT client_txn_id, connection_id, from_account, to_account,
                       amount, transaction_type, journal_status, timestamp_tsc, checksum
                FROM transaction_audit WHERE stream_id = ? AND sequence_id = ?
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, streamId);
            statement.setLong(2, record.sequenceId());
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("Sequence " + record.sequenceId()
                            + " conflicted but its existing row was not found");
                }
                boolean same = rs.getLong("client_txn_id") == record.clientTxnId()
                        && rs.getLong("connection_id") == record.connectionId()
                        && rs.getLong("from_account") == record.fromAccount()
                        && rs.getLong("to_account") == record.toAccount()
                        && rs.getLong("amount") == record.amount()
                        && rs.getShort("transaction_type") == (short) record.type()
                        && rs.getShort("journal_status") == (short) record.status()
                        && rs.getLong("timestamp_tsc") == record.timestampTsc()
                        && rs.getLong("checksum") == Integer.toUnsignedLong(record.checksum());
                if (!same) {
                    throw new IllegalStateException("Conflicting data for existing journal sequence "
                            + record.sequenceId());
                }
            }
        }
    }

    public void verifyConnection() throws SQLException {
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement("SELECT 1")) {
            statement.executeQuery().close();
        }
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }
}
