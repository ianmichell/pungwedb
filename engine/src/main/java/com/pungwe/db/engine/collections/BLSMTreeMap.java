package com.pungwe.db.engine.collections;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by ian on 27/06/2016.
 */
public class BLSMTreeMap<K, V> extends BaseMap<K, V> {

    /*
     * Initialize a table of btree maps, so that we can retrieve data and merge indexes together...
     * Merging is obviously fairly straight forward. We simply tree.putAll(otherTree) and it will automatically handle
     * the merge. Then we reset the tables array to reflect that a tree has been merged.
     */
    private BTreeMap<K,V>[] tables = new BTreeMap[0];

    private final int maxIndexSize = 1000; // 100 entries by default

    /*
     * Memory tree is the permanent memory resident btree. When it is full we flush to disk, or merge (or both)
     */
    private BTreeMap<K,V> memoryTree;

    public BLSMTreeMap(Comparator<K> keyComparator) {
        super(keyComparator);
        memoryTree = createNewTree();
    }

    private BTreeMap<K,V> createNewTree() {
        return null;
    }

    private void handleMergeOrFlush(BTreeMap<K,V> tree) {

    }

    /**
     * Retrieves a value by key from the nearest btree. This is done with the use of a bloom filter. The
     * method will check each btree when a bloom filter indicates that a record may exist, until that record is found.
     * Each time a bloom filter responds with a negative, the next filter is checked until none are left and the method
     * will return null. If the record is found at index[n], then the value is returned.
     *
     * @param key the key of the entry being looked up.
     *
     * @return the value of the entry or null if not found.
     */
    @Override
    public Entry<K, V> getEntry(K key) {
        return null;
    }

    @Override
    public Entry<K, V> putEntry(K key, V value, boolean replace) {

        if (memoryTree.size() == maxIndexSize) {
            handleMergeOrFlush(memoryTree);
            memoryTree = createNewTree();
        }

        // Place the entry into the memory tree
        return memoryTree.putEntry(key, value, replace);
    }

    @Override
    protected Iterator<Entry<K, V>> descendingIterator(Comparator<? super K> comparator, K low,
                                                       boolean lowInclusive, K high, boolean highInclusive) {
        return null;
    }

    @Override
    protected Iterator<Entry<K, V>> iterator(Comparator<? super K> comparator, K low,
                                             boolean lowInclusive, K high, boolean highInclusive) {
        return null;
    }

    @Override
    public boolean remove(Object key, Object value) {
        return false;
    }

    @Override
    public int size() {
        return (int)Math.min(Integer.MAX_VALUE, sizeLong());
    }

    public long sizeLong() {
        long size = memoryTree.sizeLong();
        for (BTreeMap<K,V> tree : tables) {
            size += tree.sizeLong();
        }
        return size;
    }

    @Override
    public void clear() {

    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return null;
    }
}
