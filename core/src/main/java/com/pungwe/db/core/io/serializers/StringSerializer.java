package com.pungwe.db.core.io.serializers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ian on 06/07/2016.
 */
public class StringSerializer implements Serializer<String> {

    @Override
    public void serialize(DataOutput out, String value) throws IOException {
        out.writeUTF(value);
    }

    @Override
    public String deserialize(DataInput in) throws IOException {
        return in.readUTF();
    }

    @Override
    public String getKey() {
        return "STR";
    }
}
