package org.g12.audit.model;

/** Data returned from Audit to Gateway after the journal row is committed. */
public record AuditAcknowledgement(
        long journalSequence,
        long transactionId,
        long connectionId,
        long auditedAtEpochMillis,
        int auditStatus,
        int journalStatus,
        int checksum
) {
    public String toWireText() {
        String result = journalStatus == 0 ? "PERSISTED" : "PERSISTED_MALFORMED";
        return "ACK|stage=AUDITED"
                + "|transactionId=" + transactionId
                + "|journalSequence=" + journalSequence
                + "|status=" + result;
    }
}
