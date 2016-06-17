package com.pungwe.db.core.io.volume;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by 917903 on 23/05/2016.
 */
public abstract class VolumeDataInput implements DataInput {

    protected final AtomicLong position;

    public VolumeDataInput(long position) {
        this.position = new AtomicLong(position);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        this.position.addAndGet(n);
        return n;
    }

    @Override
    public byte readByte() throws IOException {
        byte[] b = new byte[1];
        readFully(b);
        return b[0];
    }

    @Override
    public boolean readBoolean() throws IOException {
        byte b = readByte();
        return b == 1;
    }

    @Override
    public int readUnsignedByte() throws IOException {
        int temp = this.readByte();
        if (temp < 0) {
            throw new EOFException();
        }
        return temp;
    }

    @Override
    public short readShort() throws IOException {
        byte[] b = new byte[2];
        readFully(b, 0, 2);
        ByteBuffer buffer = ByteBuffer.wrap(b);
        return buffer.getShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        byte[] b = new byte[2];
        readFully(b, 0, 2);
        ByteBuffer buffer = ByteBuffer.wrap(b);
        short s = buffer.getShort();
        if (s < 0) {
            throw new EOFException();
        }
        return s;
    }

    @Override
    public char readChar() throws IOException {
        byte[] b = new byte[2];
        b[0] = readByte();
        b[1] = readByte();
        ByteBuffer buffer = ByteBuffer.wrap(b);
        return buffer.getChar();
    }

    @Override
    public int readInt() throws IOException {
        byte[] b = new byte[4];
        readFully(b, 0, 4);
        ByteBuffer buffer = ByteBuffer.wrap(b);
        return buffer.getInt();
    }

    @Override
    public long readLong() throws IOException {
        byte[] b = new byte[8];
        readFully(b, 0, 8);
        ByteBuffer buffer = ByteBuffer.wrap(b);
        return buffer.getLong();
    }

    @Override
    public float readFloat() throws IOException {
        byte[] b = new byte[4];
        readFully(b, 0, 4);
        ByteBuffer buffer = ByteBuffer.wrap(b);
        return buffer.getFloat();
    }

    @Override
    public double readDouble() throws IOException {
        byte[] b = new byte[8];
        readFully(b, 0, 8);
        ByteBuffer buffer = ByteBuffer.wrap(b);
        return buffer.getDouble();
    }

    @Override
    public String readLine() throws IOException {
        StringBuffer input = new StringBuffer();
        int c = -1;
        boolean eol = false;

        while (!eol) {
            switch (c = readByte()) {
                case -1:
                case '\n':
                    eol = true;
                    break;
                case '\r':
                    eol = true;
                    long cur = position.get();
                    if ((readByte()) != '\n') {
                        position.set(cur);
                    }
                    break;
                default:
                    input.append((char) c);
                    break;
            }
        }

        if ((c == -1) && (input.length() == 0)) {
            return null;
        }
        return input.toString();
    }

    @Override
    public String readUTF() throws IOException {
        return DataInputStream.readUTF(this);
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }
}
