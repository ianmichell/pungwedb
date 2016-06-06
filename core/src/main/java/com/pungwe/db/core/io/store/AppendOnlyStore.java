package com.pungwe.db.core.io.store;

import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.io.volume.Volume;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 917903 on 24/05/2016.
 */
public class AppendOnlyStore implements Store {
    protected final Volume volume;

    public AppendOnlyStore(Volume volume) {
        this.volume = volume;
    }

    @Override
    public Object get(long pointer, Serializer serializer) throws IOException {
        DataInput input = volume.getDataInput(pointer);
        return serializer.deserialize(input);
    }

    @Override
    public long add(Object value, Serializer serializer) throws IOException {
      long offset = this.volume.length();
      volume.ensureAvailable(offset);
      DataOutput output = volume.getDataOutput(offset);
      serializer.serialize(output, value);
      return offset;
    }

    @Override
    public long update(long pointer, Object value, Serializer serializer)
        throws IOException {
        // Ignore the pointer...
        long newPointer = this.volume.length();
        DataOutput output = this.volume.getDataOutput(newPointer);
        serializer.serialize(output, value);
        return newPointer;
    }
}
