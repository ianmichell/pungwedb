package com.pungwe.db.engine.collections;

import com.pungwe.db.engine.io.store.Store;
import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.engine.utils.Constants;
import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHashFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.AbstractSet;
import java.util.Iterator;
import java.io.IOException;
import java.util.Map;

public class BigHashSet<E> extends AbstractSet<E> {

    protected final Store store;
    protected final long maxEntries;
    protected final BTreeMap btree;
    protected final Serializer serializer;

    public BigHashSet(Store store, Serializer serializer, long maxNodeSize) throws IOException {
        this(store, serializer, -1l, maxNodeSize, -1l);
    }
    
    public BigHashSet(Store store, Serializer serializer, long maxEntries, long maxNodeSize, long pointer) throws IOException {
        this.store = store;
        this.maxEntries = maxEntries;
        this.serializer = serializer;
        // Setup the BTree!
        this.btree = new BTreeMap(store, (o1, o2) -> {
            if (o1 == null && o2 != null) {
                return 1;
            } else if (o1 != null && o2 == null) {
                return -1;
            } else if (o1 == null && o2 == null) {
                return 0;
            }
            return Long.compare((Long)o1, (Long)o2);
        }, new ObjectSerializer(), new ObjectSerializer(), maxNodeSize, pointer);
    }

    public boolean add(E value) {
        if (value == null) {
            throw new IllegalArgumentException("This set cannot store null values");
        }
        try {
            long hash = generateHashCode(value);
            if (btree.containsKey(hash)) {
                return false;
            }
            // Store as a record regardless
            long position = store.add(value, serializer);
            // Store the position and hash key in the btree
            return btree.put(hash, position) != null;
        } catch (IOException ex) {
            throw new RuntimeException("Could not generate hash for value");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean contains(Object o) {
        try {
            long hash = generateHashCode((E)o);
            // Store the value and record the position
            return btree.containsKey(hash);
        } catch (IOException ex) {
            throw new RuntimeException("Could not generate hash for value");
        }
    }

    public int size() {
        return btree.size();
    }

    public long sizeLong() {
        return btree.sizeLong();
    }

    public Iterator<E> iterator() {
        return new BigHashSetIterator<>();
    }

    private class BigHashSetIterator<E> implements Iterator<E> {
        private final Iterator<Map.Entry<Object, Object>> it;

        public BigHashSetIterator() {
            it = btree.iterator();
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public E next() {
            Map.Entry<Object, Object> entry = it.next();
            long position = (long)entry.getValue();
            try {
                E value = (E) store.get(position, serializer);
                return value;
            } catch (IOException ex) {
                return null;
            }
        }

    }

    private long generateHashCode(E value) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        serializer.serialize(out, value);

        XXHashFactory factory = XXHashFactory.fastestInstance();
        StreamingXXHash64 hash64 = factory.newStreamingHash64(Constants.HASH_SEED);
        byte[] toHash = bytes.toByteArray();
        hash64.update(toHash, 0, toHash.length);
        return hash64.getValue();
    }
}