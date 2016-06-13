package com.pungwe.db.core.io.store;

import com.pungwe.db.core.io.serializers.Serializer;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

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
}
