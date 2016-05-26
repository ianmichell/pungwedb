package com.pungwe.db.core.collections;

import com.pungwe.db.core.io.store.Store;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by ian on 25/05/2016.
 */
public class BTreeMap<K, V> extends BaseMap<K, V> {

    final Store store;

    public BTreeMap(Store store, Comparator<K> keyComparator) {
        super(keyComparator);
        this.store = store;
    }

    @Override
    public Entry<K, V> getEntry(K key) {
        return null;
    }

    @Override
    public Entry<K, V> putEntry(K key, V value) {
        return null;
    }

    @Override
    public Entry<K, V> replaceEntry(K key, V value) {
        return null;
    }

    @Override
    protected Iterator<Entry<K, V>> iterator(Comparator<? super K> comparator, K low, boolean lowInclusive, K high, boolean highInclusive) {
        return null;
    }

    @Override
    public boolean remove(Object key, Object value) {
        return false;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public void clear() {

    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return null;
    }
}
