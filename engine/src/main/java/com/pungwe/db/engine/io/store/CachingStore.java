package com.pungwe.db.engine.io.store;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.engine.io.volume.Volume;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by 917903 on 29/06/2016.
 */
public class CachingStore implements Store {

    private final ReentrantReadWriteLock lock;
    private final Store store;
    private final Cache<Long, Object> cache;

    public CachingStore(Store store, int cacheSize) {
        this.lock = new ReentrantReadWriteLock();
        this.store = store;
        this.cache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
    }

    @Override
    public Object get(final long pointer, final Serializer serializer) throws IOException {
        lock.readLock().lock();
        try {
            return cache.get(pointer, () -> store.get(pointer, serializer));
        } catch (ExecutionException e) {
            throw new IOException(e);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long add(Object value, Serializer serializer) throws IOException {
        lock.writeLock().lock();
        try {
            long pointer = store.add(value, serializer);
            cache.put(pointer, value);
            return pointer;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long update(long pointer, Object value, Serializer serializer) throws IOException {
        lock.writeLock().lock();
        try {
            long newPointer = store.update(pointer, value, serializer);
            if (pointer != newPointer) {
                cache.invalidate(pointer);
            }
            cache.put(newPointer, value);
            return newPointer;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long size() {
        return store.size();
    }

    @Override
    public void commit() throws IOException {

    }

    @Override
    public long getPosition() {
        return store.getPosition();
    }

    @Override
    public void rollback() throws IOException {
        store.rollback();
        cache.invalidateAll();
    }

    @Override
    public void clear() throws IOException {
        store.clear();
    }

    @Override
    public Volume getVolume() {
        return store.getVolume();
    }

    @Override
    public Iterator<Object> iterator() {
        return store.iterator();
    }
}
