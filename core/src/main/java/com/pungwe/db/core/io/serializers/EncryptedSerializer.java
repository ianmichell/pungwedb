package com.pungwe.db.core.io.serializers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ian on 20/06/2016.
 */
@Deprecated
public class EncryptedSerializer implements Serializer {

    private final Serializer serializer;

    public EncryptedSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    @Override
    public void serialize(DataOutput out, Object value) throws IOException {
        out.writeUTF(getKey());
    }

    @Override
    public Object deserialize(DataInput in) throws IOException {
        return null;
    }

    @Override
    public String getKey() {
        return "ENC:" + serializer.getKey();
    }
}
