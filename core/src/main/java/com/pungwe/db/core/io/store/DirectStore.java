package com.pungwe.db.core.io.store;

import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.io.volume.Volume;

import java.util.concurrent.atomic.AtomicLong;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;


/**
 * Created by ian on 27/05/2016.
 */
public class DirectStore implements Store {

    protected final Volume volume;

    public DirectStore(Volume volume, long maxEntries) throws IOException {
        this.volume = volume;
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
        // Create a byte array stream
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        serializer.serialize(out, value);
        out.flush();
        output.writeByte('R');
        output.writeInt(bytes.toByteArray().length);
        output.write(bytes.toByteArray());
        return offset;
    }

    @Override
    public long update(long pointer, Object value, Serializer serializer) throws IOException {
        return 0;
    }
}
