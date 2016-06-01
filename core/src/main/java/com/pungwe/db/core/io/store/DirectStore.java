package com.pungwe.db.core.io.store;

import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.io.volume.Volume;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ian on 27/05/2016.
 */
public class DirectStore implements Store {

    protected final Volume volume;

    public DirectStore(Volume volume, long maxEntries) {
        this.volume = volume;
    }

    @Override
    public Object get(long pointer, Serializer serializer) {
        return null;
    }

    @Override
    public long add(Object value, Serializer serializer) {
        return 0;
    }

    @Override
    public long update(long pointer, Object value, Serializer serializer) {
        return 0;
    }
}
