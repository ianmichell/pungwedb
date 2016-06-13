package com.pungwe.db.core.collections;

import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.io.store.Store;

import java.io.IOException;
import java.util.*;

/**
 * Created by ian on 12/06/2016.
 */
public class BigTreeSet<E> extends AbstractSet<E> implements SortedSet<E> {

    protected final Store store;
    protected final long maxEntries;
    protected final BTreeMap btree;
    protected final Serializer serializer;
    protected final Comparator<E> comparator;

    public BigTreeSet(Store store, Comparator<E> comparator, Serializer serializer, long maxNodeSize)
            throws IOException {
        this(store, comparator, serializer, -1l, maxNodeSize, -1l);
    }

    public BigTreeSet(Store store, Comparator<E> comparator, Serializer serializer, long maxEntries,
                      long maxNodeSize, long pointer) throws IOException {
        this.store = store;
        this.maxEntries = maxEntries;
        this.serializer = serializer;
        this.comparator = comparator;
        // Setup the BTree!
        this.btree = new BTreeMap(store, comparator, new ObjectSerializer(), new ObjectSerializer(), maxNodeSize, -1l);
    }

    public boolean add(E value) {
        if (value == null) {
            throw new IllegalArgumentException("This set cannot store null values");
        }
        if (btree.containsKey(value)) {
            return false;
        }
        // Store the position and hash key in the btree
        return btree.put(value, 0) != null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean contains(Object o) {
        return btree.containsKey(o);
    }

    @Override
    public Iterator<E> iterator() {
        return new BigTreeSetIterator<>(this.btree);
    }

    @Override
    public int size() {
        return btree.size();
    }

    @Override
    public Comparator<? super E> comparator() {
        return comparator;
    }

    @Override
    public SortedSet<E> subSet(E fromElement, E toElement) {
        final NavigableMap<Object, Object> subMap = btree.subMap(fromElement, toElement);
        return new SubSet(subMap);
    }

    @Override
    public SortedSet<E> headSet(E toElement) {
        final NavigableMap<Object, Object> subMap = btree.headMap(toElement);
        return new SubSet(subMap);
    }

    @Override
    public SortedSet<E> tailSet(E fromElement) {
        final NavigableMap<Object, Object> subMap = btree.tailMap(fromElement);
        return new SubSet(subMap);
    }

    @Override
    public E first() {
        return (E)btree.firstKey();
    }

    @Override
    public E last() {
        return (E)btree.lastKey();
    }

    private class SubSet extends AbstractSet<E> implements SortedSet<E> {
        private final NavigableMap<Object, Object> subMap;

        public SubSet(NavigableMap<Object, Object> subMap) {
            this.subMap = subMap;
        }

        @Override
        public Iterator<E> iterator() {
            return new BigTreeSetIterator<>(this.subMap);
        }

        @Override
        public int size() {
            return this.subMap.size();
        }

        @Override
        public Comparator<? super E> comparator() {
            return BigTreeSet.this.comparator;
        }

        @Override
        public SortedSet<E> subSet(E fromElement, E toElement) {
            NavigableMap<Object, Object> subMap = (NavigableMap<Object, Object>)this.subMap
                    .subMap(fromElement, toElement);
            return new SubSet(subMap);
        }

        @Override
        public SortedSet<E> headSet(E toElement) {
            NavigableMap<Object, Object> subMap = (NavigableMap<Object, Object>)this.subMap
                    .headMap(toElement);
            return new SubSet(subMap);
        }

        @Override
        public SortedSet<E> tailSet(E fromElement) {
            NavigableMap<Object, Object> subMap = (NavigableMap<Object, Object>)this.subMap
                    .tailMap(fromElement);
            return new SubSet(subMap);
        }

        @Override
        public E first() {
            return (E)subMap.firstKey();
        }

        @Override
        public E last() {
            return (E)subMap.lastKey();
        }
    }

    private class BigTreeSetIterator<E> implements Iterator<E> {
        private final Iterator<Object> it;

        public BigTreeSetIterator(NavigableMap<Object, Object> map) {
            it = map.keySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public E next() {
            return (E)it.next();
        }

    }
}
