package com.pungwe.db.core.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;

/**
 * Created by 917903 on 04/07/2016.
 */
public class ByteBufferInputStream extends InputStream {

    private final ByteBuffer buffer;

    public ByteBufferInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
        // Set buffer to position 0
        this.buffer.position(0);
    }

    @Override
    public int read() throws IOException {
        return buffer.get();
    }

    @Override
    public long skip(long n) throws IOException {
        if (buffer.remaining() < n) {
            throw new IOException("Could not skip " + n + " bytes");
        }
        buffer.position(buffer.position() + (int)n);
        return buffer.position();
    }

    @Override
    public int available() throws IOException {
        return buffer.remaining();
    }

    @Override
    public synchronized void mark(int readlimit) {
        buffer.mark();
    }

    @Override
    public synchronized void reset() throws IOException {
        try {
            buffer.reset();
        } catch (InvalidMarkException ex) {
            buffer.rewind();
        }
    }

    @Override
    public boolean markSupported() {
        return true;
    }
}
