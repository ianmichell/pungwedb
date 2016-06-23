package com.pungwe.db.core.io.serializers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 917903 on 24/05/2016.
 */
public interface Serializer {

    void serialize(DataOutput out, Object value) throws IOException;
    Object deserialize(DataInput in) throws IOException;
    String getKey();
}
