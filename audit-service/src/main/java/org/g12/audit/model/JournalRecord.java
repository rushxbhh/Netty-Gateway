package org.g12.audit.model;

/** Immutable decoded representation of one 64-byte C++ JournalSlot. */
public record JournalRecord(
        long sequenceId,
        long timestampTsc,
        long clientTxnId,
        long connectionId,
        long fromAccount,
        long toAccount,
        long amount,
        int type,
        int status,
        int reserved,
        int checksum
) {
    public boolean isSequencedOk() {
        return status == 0;
    }
}
