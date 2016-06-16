package com.pungwe.db.core.io.volume;

import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertEquals;

/**
 * Created by 917903 on 23/05/2016.
 */
public class DirectByteBufferVolumeTest {

    @Test
    public void testReadWriteByte() throws Exception {

        DirectByteBufferVolume volume = new DirectByteBufferVolume("bytes", false, 20, Integer.MAX_VALUE);
        DataOutput output = volume.getDataOutput(0);
        output.writeByte(100);
        DataInput input = volume.getDataInput(0);
        byte result = input.readByte();

        assertEquals(100, result);
    }

    @Test(expected = EOFException.class)
    public void testReadWriteNegativeUnsignedByte() throws Exception {
        DirectByteBufferVolume volume = new DirectByteBufferVolume("bytes", false, 20, Integer.MAX_VALUE);
        DataOutput output = volume.getDataOutput(0);
        output.writeByte(-100);
        DataInput input = volume.getDataInput(0);
        byte result = (byte) input.readUnsignedByte();

        assert false;
    }

    @Test
    public void testReadWriteShort() throws Exception {

        DirectByteBufferVolume volume = new DirectByteBufferVolume("bytes", false, 20, Integer.MAX_VALUE);
        DataOutput output = volume.getDataOutput(0);
        output.writeShort(100);
        DataInput input = volume.getDataInput(0);
        short result = input.readShort();

        assertEquals(100, result);
    }

    @Test(expected = EOFException.class)
    public void testReadWriteNegativeUnsignedShort() throws Exception {
        DirectByteBufferVolume volume = new DirectByteBufferVolume("bytes", false, 20, Integer.MAX_VALUE);
        DataOutput output = volume.getDataOutput(0);
        output.writeShort(-100);
        DataInput input = volume.getDataInput(0);
        short result = (short) input.readUnsignedShort();
        assert false;
    }

    @Test
    public void testReadWriteChar() throws IOException {
        DirectByteBufferVolume volume = new DirectByteBufferVolume("bytes", false, 20, Integer.MAX_VALUE);
        DataOutput output = volume.getDataOutput(0);
        output.writeChar('z');
        DataInput input = volume.getDataInput(0);
        char result = input.readChar();

        assertEquals('z', result);
    }

    @Test
    public void testReadWriteUTF8String() throws Exception {

        DirectByteBufferVolume volume = new DirectByteBufferVolume("bytes", false, 20, Integer.MAX_VALUE);
        DataOutput output = volume.getDataOutput(0);
        output.writeUTF("This is a UTF8 String test");
        DataInput input = volume.getDataInput(0);
        String result = input.readUTF();

        assertEquals("This is a UTF8 String test", result);
    }

    @Test
    public void testMultipleIntegerWrite() throws Exception {
        DirectByteBufferVolume volume = new DirectByteBufferVolume("bytes", false, 20, Integer.MAX_VALUE);
        DataOutput output = volume.getDataOutput(0);
        for (int i = 0; i < 10000; i++) {
            output.writeInt(i);
        }
        DataInput input = volume.getDataInput(40);
        int result = input.readInt();
        assertEquals(10, result);
    }
}
