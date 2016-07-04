package com.pungwe.db.core.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Created by 917903 on 04/07/2016.
 */
public class ByteBufferOutputStream extends OutputStream {

    private final ByteBuffer buffer;

    public ByteBufferOutputStream(ByteBuffer buffer) {
        this.buffer = buffer;
        this.buffer.position(0);
    }

    @Override
    public void write(int b) throws IOException {
        buffer.put((byte)b);
    }
}
