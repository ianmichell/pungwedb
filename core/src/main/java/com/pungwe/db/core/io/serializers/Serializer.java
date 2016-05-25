package com.pungwe.db.core.io.serializers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 917903 on 24/05/2016.
 */
public interface Serializer<T> {

    void serialize(DataOutput out, T value) throws IOException;
    T deserialize(DataInput in) throws IOException;

}
