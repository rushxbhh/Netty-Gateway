package org.java;

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
        String status = journalStatus == 0 ? "PERSISTED" : "PERSISTED_MALFORMED";
        return "ACK|stage=AUDITED"
                + "|transactionId=" + transactionId
                + "|journalSequence=" + journalSequence
                + "|status=" + status;
    }
}
