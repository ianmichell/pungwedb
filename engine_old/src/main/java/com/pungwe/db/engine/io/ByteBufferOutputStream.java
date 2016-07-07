package com.pungwe.db.engine.io;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Created by 917903 on 04/07/2016.
 */
public class ByteBufferOutputStream extends OutputStream implements DataOutput {

    private final ByteBufferDataOutput out;

    public ByteBufferOutputStream(ByteBuffer buffer) {
        out = new ByteBufferDataOutput(buffer, buffer.position());
    }

    @Override
    public void write(int b) throws IOException {
        out.writeByte(b);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        out.writeBoolean(v);
    }

    @Override
    public void writeByte(int v) throws IOException {
        out.writeByte(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        out.writeShort(v);
    }

    @Override
    public void writeChar(int v) throws IOException {
        out.writeChar(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        out.writeInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        out.writeLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        out.writeFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        out.writeDouble(v);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        out.writeBytes(s);
    }

    @Override
    public void writeChars(String s) throws IOException {
        out.writeChars(s);
    }

    @Override
    public void writeUTF(String s) throws IOException {
        out.writeUTF(s);
    }

    private static class ByteBufferDataOutput extends AbstractDataOutput {

        private ByteBuffer buffer;

        public ByteBufferDataOutput(ByteBuffer byteBuffer, long position) {
            super(position);
            buffer = byteBuffer;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            // start position
            int remaining = len - off;
            if (remaining <= 0) {
                return;
            }
            int s = (int) position.get();
            int n = Math.min(buffer.remaining(), remaining);
            for (int j = off; j < (off + n); j++) {
                buffer.put(b[j]);
            }
            position.addAndGet(n);
        }
    }
}
