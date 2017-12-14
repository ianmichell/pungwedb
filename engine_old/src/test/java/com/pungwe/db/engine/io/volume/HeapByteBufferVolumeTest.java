package com.pungwe.db.engine.io.volume;

import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertEquals;

/**
 * Created by 917903 on 23/05/2016.
 */
public class HeapByteBufferVolumeTest {

    @Test
    public void testReadWriteByte() throws Exception {
        HeapByteBufferVolume volume = new HeapByteBufferVolume("bytes", false, 20, Integer.MAX_VALUE);
        DataOutput output = volume.getDataOutput(0);
        output.writeByte(100);
        DataInput input = volume.getDataInput(0);
        byte result = input.readByte();

        assertEquals(100, result);
    }

    @Test(expected = EOFException.class)
    public void testReadWriteNegativeUnsignedByte() throws Exception {

        HeapByteBufferVolume volume = new HeapByteBufferVolume("bytes", false, 20, Integer.MAX_VALUE);
        DataOutput output = volume.getDataOutput(0);
        output.writeByte(-100);
        DataInput input = volume.getDataInput(0);
        byte result = (byte) input.readUnsignedByte();

        assert false;
    }

    @Test
    public void testReadWriteShort() throws Exception {

        HeapByteBufferVolume volume = new HeapByteBufferVolume("bytes", false, 20, Integer.MAX_VALUE);
        DataOutput output = volume.getDataOutput(0);
        output.writeShort(100);
        DataInput input = volume.getDataInput(0);
        short result = input.readShort();

        assertEquals(100, result);
    }

    @Test(expected = EOFException.class)
    public void testReadWriteNegativeUnsignedShort() throws Exception {
        HeapByteBufferVolume volume = new HeapByteBufferVolume("bytes", false, 20, Integer.MAX_VALUE);
        DataOutput output = volume.getDataOutput(0);
        output.writeShort(-100);
        DataInput input = volume.getDataInput(0);
        short result = (short) input.readUnsignedShort();

        assert false;
    }

    @Test
    public void testReadWriteChar() throws IOException {
        HeapByteBufferVolume volume = new HeapByteBufferVolume("bytes", false, 20, Integer.MAX_VALUE);
        DataOutput output = volume.getDataOutput(0);
        output.writeChar('z');
        DataInput input = volume.getDataInput(0);
        char result = input.readChar();

        assertEquals('z', result);
    }

    @Test
    public void testReadWriteUTF8String() throws Exception {

        HeapByteBufferVolume volume = new HeapByteBufferVolume("bytes", false, 20, Integer.MAX_VALUE);
        DataOutput output = volume.getDataOutput(0);
        output.writeUTF("This is a UTF8 String test");
        DataInput input = volume.getDataInput(0);
        String result = input.readUTF();

        assertEquals("This is a UTF8 String test", result);
    }

    @Test
    public void testMultipleIntegerWrite() throws Exception {
        HeapByteBufferVolume volume = new HeapByteBufferVolume("bytes", false, 20, Integer.MAX_VALUE);
        DataOutput output = volume.getDataOutput(0);
        for (int i = 0; i < 10000; i++) {
            output.writeInt(i);
        }
        DataInput input = volume.getDataInput(40);
        int result = input.readInt();
        assertEquals(10, result);
    }

    @Test
    public void testWriteToTwoSegments() throws Exception {
        byte[] bytes = new byte[(1 << 20) + 29470];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = 'A';
        }
        HeapByteBufferVolume volume = new HeapByteBufferVolume("bytes", false, 20, Integer.MAX_VALUE);
        DataOutput output = volume.getDataOutput(2447);
        output.write(bytes);

        byte[] result = new byte[bytes.length];
        DataInput input = volume.getDataInput(2447);
        input.readFully(result);
        for (int i = 0; i < bytes.length; i++) {
            try {
                assertEquals(bytes[i], result[i]);
            } catch (AssertionError error) {
                System.out.println("i: " + i);
                throw error;
            }
        }
    }

    private static void printArray(byte[] anArray) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < anArray.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(anArray[i]);
        }
        System.out.println(sb.toString());
    }
}
