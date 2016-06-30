package com.pungwe.db.engine.io.store;

import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.engine.io.volume.Volume;

import java.io.IOException;

/**
 * Created by 917903 on 24/05/2016.
 */
public interface Store extends Iterable<Object> {
    Object get(long pointer, Serializer serializer) throws IOException;
    long add(Object value, Serializer serializer) throws IOException;
    long update(long pointer, Object value, Serializer serializer) throws IOException;
    long size();
    void commit() throws IOException;
    void rollback() throws IOException;
    void clear() throws IOException;
    Volume getVolume();
    long getPosition();

    default void appendVolume(Volume volume) {
        this.getVolume().getDataOutput(getPosition()).
    }
}
