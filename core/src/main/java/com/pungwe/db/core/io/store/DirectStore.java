package com.pungwe.db.core.io.store;

import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.io.volume.Volume;
import com.pungwe.db.core.utils.Constants;

import java.util.concurrent.atomic.AtomicLong;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;


/**
 * Created by ian on 27/05/2016.
 */
public class DirectStore implements Store {

    protected final Volume volume;
    protected final long maxEntries;

    public DirectStore(Volume volume) throws IOException {
        this(volume, -1l);
    }

    public DirectStore(Volume volume, long maxEntries) throws IOException {
        this.volume = volume;
        this.maxEntries = maxEntries;
    }

    @Override
    public Object get(long pointer, Serializer serializer) throws IOException {
        DataInput input = this.volume.getDataInput(pointer);
        byte type = input.readByte();
        int length = input.readInt();
        if (type != 'R') {
            throw new IOException("Invalid record entry at offset: " + pointer);
        }
        return serializer.deserialize(input);
    }

    @Override
    public long add(Object value, Serializer serializer) throws IOException {
        long offset = this.volume.length();
        this.volume.ensureAvailable(offset);
        DataOutput output = this.volume.getDataOutput(offset);
        byte[] data = writeRecord(serializer, value);
        output.writeByte('R');
        output.writeInt(data.length);
        output.write(data);

        // Mod for block size
        int remainder = data.length % Constants.BLOCK_SIZE;
        if (remainder > 0) {
            output.write(new byte[remainder]);
        }

        return offset;
    }

    @Override
    public long update(long pointer, Object value, Serializer serializer) throws IOException {
        DataInput input = this.volume.getDataInput(pointer);
        if (input.readByte() != 'R') {
            throw new IOException("Offset: " + pointer + " is not a valid record");
        }
        int size = input.readInt();
        byte[] bytes = writeRecord(serializer, value);
        // FIXME: Check padding...
        if (size < bytes.length) {
            // If the existing size is smaller than the document append the the end of the volume...
            return add(value, serializer);
        }
        // Update the existing record
        DataOutput output = this.volume.getDataOutput(pointer);
        output.writeByte('R');
        output.writeInt(bytes.length);
        output.write(bytes);
        // Mod for block size
        int remainder = bytes.length % Constants.BLOCK_SIZE;
        if (remainder > 0) {
            output.write(new byte[remainder]);
        }
        return pointer;
    }

    private byte[] writeRecord(Serializer serializer, Object value) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteStream);
        try {
            serializer.serialize(out, value);
            // Return the array of bytes from the output stream.
            return byteStream.toByteArray();
        } finally {
            out.close();
            byteStream.close();
        }
    }
}
