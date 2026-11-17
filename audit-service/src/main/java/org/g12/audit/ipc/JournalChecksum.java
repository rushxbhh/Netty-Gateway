package org.g12.audit.ipc;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

public final class JournalChecksum {
    private JournalChecksum() {
    }

    public static int crc32c(ByteBuffer source, int offset, int length) {
        ByteBuffer view = source.duplicate();
        view.position(offset).limit(offset + length);
        CRC32C crc = new CRC32C();
        crc.update(view.slice());
        return (int) crc.getValue();
    }
}
