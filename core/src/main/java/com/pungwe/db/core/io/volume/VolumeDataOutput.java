package com.pungwe.db.core.io.volume;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by 917903 on 23/05/2016.
 */
public abstract class VolumeDataOutput implements DataOutput {

    protected final AtomicLong position;

    public VolumeDataOutput(long position) {
        this.position = new AtomicLong(position);
    }

    @Override
    public void write(int b) throws IOException {
        writeByte(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        write(v ? 1 : 0);
    }
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        for (int i = off; i < len; i++) {
            write(b[i]);
        }
    }

    @Override
    public void writeShort(int v) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.putShort((short)v);
        buffer.flip();
        write(buffer.array(), 0, 2);
    }

    @Override
    public void writeChar(int v) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.putChar((char)v);
        buffer.flip();
        write(buffer.array(), 0, 2);
    }

    @Override
    public void writeInt(int v) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(v);
        buffer.flip();
        write(buffer.array(), 0, 4);
    }

    @Override
    public void writeLong(long v) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(v);
        buffer.flip();
        write(buffer.array(), 0, 8);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putFloat(v);
        buffer.flip();
        write(buffer.array(), 0, 4);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putDouble(v);
        buffer.flip();
        write(buffer.array(), 0, 8);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        byte bytes[] = new byte[s.length()];
        for (int index = 0; index < s.length(); index++) {
            bytes[index] = (byte) (s.charAt(index) & 0xFF);
        }
        write(bytes);
    }

    @Override
    public void writeChars(String s) throws IOException {
        byte newBytes[] = new byte[s.length() * 2];
        for (int index = 0; index < s.length(); index++) {
            int newIndex = index == 0 ? index : index * 2;
            newBytes[newIndex] = (byte) ((s.charAt(index) >> 8) & 0xFF);
            newBytes[newIndex + 1] = (byte) (s.charAt(index) & 0xFF);
        }
        write(newBytes);
    }

    @Override
    public void writeUTF(String s) throws IOException {
        int utfCount = 0, length = s.length();
        for (int i = 0; i < length; i++) {
            int charValue = s.charAt(i);
            if (charValue > 0 && charValue <= 127) {
                utfCount++;
            } else if (charValue <= 2047) {
                utfCount += 2;
            } else {
                utfCount += 3;
            }
        }
        if (utfCount > 65535) {
            throw new UTFDataFormatException(); //$NON-NLS-1$
        }
        byte utfBytes[] = new byte[utfCount + 2];
        int utfIndex = 2;
        for (int i = 0; i < length; i++) {
            int charValue = s.charAt(i);
            if (charValue > 0 && charValue <= 127) {
                utfBytes[utfIndex++] = (byte) charValue;
            } else if (charValue <= 2047) {
                utfBytes[utfIndex++] = (byte) (0xc0 | (0x1f & (charValue >> 6)));
                utfBytes[utfIndex++] = (byte) (0x80 | (0x3f & charValue));
            } else {
                utfBytes[utfIndex++] = (byte) (0xe0 | (0x0f & (charValue >> 12)));
                utfBytes[utfIndex++] = (byte) (0x80 | (0x3f & (charValue >> 6)));
                utfBytes[utfIndex++] = (byte) (0x80 | (0x3f & charValue));
            }
        }
        utfBytes[0] = (byte) (utfCount >> 8);
        utfBytes[1] = (byte) utfCount;
        write(utfBytes);
    }
}
