package com.pungwe.db.core.io.store;

import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.io.volume.Volume;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 917903 on 24/05/2016.
 */
public class AppendOnlyStore extends DirectStore {

    public AppendOnlyStore(Volume volume) throws IOException {
        super(volume);
    }

    public AppendOnlyStore(Volume volume, long maxEntries) throws IOException {
        super(volume, maxEntries);
    }

    @Override
    public long update(long pointer, Object value, Serializer serializer)
        throws IOException {
        return add(value, serializer);
    }
}
