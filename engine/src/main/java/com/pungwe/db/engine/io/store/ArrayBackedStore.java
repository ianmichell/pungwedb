package com.pungwe.db.engine.io.store;

import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.registry.SerializerRegistry;
import com.pungwe.db.engine.io.volume.Volume;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ian on 30/06/2016.
 */
public class ArrayBackedStore implements Store {

    protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    protected final ArrayList<byte[]> items = new ArrayList<>();

    public ArrayBackedStore() {
        items.add(null); // We can't have a 0....
    }

    @Override
    public Object get(long pointer, Serializer serializer) throws IOException {
        lock.readLock().lock();
        try {
            ByteArrayInputStream bytes = new ByteArrayInputStream(items.get((int)pointer));
            DataInputStream dataIn = new DataInputStream(bytes);
            return serializer.deserialize(dataIn);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long add(Object value, Serializer serializer) throws IOException {
        lock.writeLock().lock();
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(out);
            serializer.serialize(dataOut, value);
            items.add(out.toByteArray());
            return items.size() - 1;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long update(long pointer, Object value, Serializer serializer) throws IOException {
        lock.writeLock().lock();
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(out);
            serializer.serialize(dataOut, value);
            items.set((int)pointer, out.toByteArray());
            return pointer;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long size() {
        return items.size();
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
        return null;
    }

    @Override
    public long getPosition() {
        return items.size();
    }

    @Override
    public Iterator<Object> iterator() {
        return new ItemIterator();
    }

    private class ItemIterator implements Iterator<Object> {
        final Iterator<byte[]> it;

        public ItemIterator() {
            it = items.iterator();
        }
        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public Object next() {
            try {
                byte[] b = it.next();
                if (b == null) {
                    return null;
                }
                ByteArrayInputStream bytesIn = new ByteArrayInputStream(b);
                DataInputStream dataIn = new DataInputStream(bytesIn);
                String serializerKey = dataIn.readUTF();
                Serializer serializer = SerializerRegistry.getIntance().getByKey(serializerKey);
                if (serializer == null) {
                    throw new IllegalArgumentException("Could not find appropriate serializer");
                }
                dataIn.reset();
                return serializer.deserialize(dataIn);
            } catch (IOException ex) {
                return null;
            }
        }
    }
}
