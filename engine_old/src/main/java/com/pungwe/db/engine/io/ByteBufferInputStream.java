package com.pungwe.db.engine.io;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Created by 917903 on 04/07/2016.
 */
public class ByteBufferInputStream extends InputStream implements DataInput {

    private final ByteBufferDataInput input;

    public ByteBufferInputStream(ByteBuffer buffer) {
        ByteBuffer input = buffer.asReadOnlyBuffer();
        this.input = new ByteBufferDataInput(input, buffer.position());
    }

    @Override
    public int read() throws IOException {
        return input.readByte();
    }

    @Override
    public synchronized void reset() throws IOException {
        input.position.set(0);
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        input.readFully(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        input.readFully(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        return input.skipBytes(n);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return input.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return input.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return input.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException {
        return input.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return input.readUnsignedByte();
    }

    @Override
    public char readChar() throws IOException {
        return input.readChar();
    }

    @Override
    public int readInt() throws IOException {
        return input.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return input.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return input.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        return input.readDouble();
    }

    @Override
    public String readLine() throws IOException {
        return input.readLine();
    }

    @Override
    public String readUTF() throws IOException {
        return input.readUTF();
    }

    @Override
    public long skip(long n) throws IOException {
        return skipBytes((int)n);
    }

    private static class ByteBufferDataInput extends AbstractDataInput {

        final ByteBuffer buffer;

        public ByteBufferDataInput(ByteBuffer buffer, long position) {
            super(position);
            this.buffer = buffer;
        }

        @Override
        public void readFully(byte[] b, int off, int len) throws IOException {
            int remaining = len - off;
            if (remaining <= 0) {
                return;
            }
            int s = (int)position.get();
            int n = Math.min(buffer.remaining(), remaining);
            buffer.position(s);
            for (int j = off; j < (off + n); j++) {
                b[j] = buffer.get();
            }
            position.addAndGet(n);
        }
    }
}
