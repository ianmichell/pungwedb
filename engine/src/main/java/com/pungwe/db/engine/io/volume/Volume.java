package com.pungwe.db.engine.io.volume;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 */
public interface Volume extends Closeable {

    byte[] CLEAR = new byte[1024];

    String name();

    /**
     * Returns the length of the file, which is always a multiple of Constants.BLOCK_SIZE.
     *
     * @return the length of the file
     */
    long length() throws IOException;

    DataOutput getDataOutput(long offset);
    DataInput getDataInput(long offset);

    long getPositionLimit();
    void ensureAvailable(long offset) throws IOException;
    boolean isClosed();
    void clear(long startOffset, long endOffset) throws IOException;

}
