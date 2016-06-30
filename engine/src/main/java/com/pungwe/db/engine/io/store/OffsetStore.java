package com.pungwe.db.engine.io.store;

import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.engine.io.volume.Volume;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A store that uses an offset to smooth the use of memory segments to disk without modification.
 */
public class OffsetStore implements Store {

    protected final Store store;
    protected final long offset;
    protected final long size;

    public OffsetStore(Store store, long offset, long size) {
        // Get current position of the store
        this.offset = offset;
        // Sets the maximum size of the store.
        this.size = size;
        // Sets the store.
        this.store = store;
    }

    @Override
    public Object get(long pointer, Serializer serializer) throws IOException {
        return store.get(pointer - offset, serializer);
    }

    @Override
    public long add(Object value, Serializer serializer) throws IOException {
        return offset + store.add(value, serializer);
    }

    @Override
    public long update(long pointer, Object value, Serializer serializer) throws IOException {
        return offset + store.update(pointer - offset, value, serializer);
    }

    @Override
    public long size() {
        return store.size();
    }

    @Override
    public void commit() throws IOException {

    }

    @Override
    public void rollback() throws IOException {

    }

    @Override
    public void clear() throws IOException {

    }

    @Override
    public Volume getVolume() {
        return this.store.getVolume();
    }

    @Override
    public long getPosition() {
        return offset + store.getPosition();
    }

    @Override
    public Iterator<Object> iterator() {
        return store.iterator();
    }
}
