package com.pungwe.db.core.io.serializers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 917903 when 20/07/2016.
 */
public class ByteSerializer implements Serializer<byte[]> {

    @Override
    public void serialize(DataOutput out, byte[] value) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    @Override
    public byte[] deserialize(DataInput in) throws IOException {
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    @Override
    public String getKey() {
        return "BYTES";
    }
}
