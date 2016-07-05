package com.pungwe.db.engine.collections;

import com.pungwe.db.core.concurrent.Promise;
import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.engine.io.store.*;
import com.pungwe.db.engine.io.volume.HeapByteBufferVolume;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

// FIXME: This needs a bit of work...
/**
 * Created by ian on 27/06/2016.
 */
public final class BLSMTreeMap<K, V> extends BaseMap<K, V> {

    /*
     * Initialize a table of btree maps, so that we can retrieve data and merge indexes together...
     * Merging is obviously fairly straight forward. We simply tree.putAll(otherTree) and it will automatically handle
     * the merge. Then we reset the tables array to reflect that a tree has been merged.
     */
    private BTreeMap<K, V>[] tables = new BTreeMap[0];

    /**
     * Background thread that executes flushes to disk. This is multithreaded by the number of available CPU
     */
    final ExecutorService flushExecutor = Executors.newSingleThreadExecutor();

    private final long maxIndexSize; // 1000 entries by default

    private final Store store;

    /*
     * Memory tree is the permanent memory resident btree. When it is full we flush to disk, or merge (or both)
     */
    private volatile BTreeMap<K, V> memoryTree;

    private final long maxNodeSize;
    private volatile long pointer;
    private final Serializer keySerializer;
    private final Serializer valueSerializer;
    private final AtomicInteger memoryTrees = new AtomicInteger();

    /**
     * Creates an instance of this bLSM Tree.
     *
     * @param store         the store used to manage the on disk(?) indexes
     * @param keyComparator the key comparator
     * @param maxIndexSize  the maximum size of an index in memory
     * @param maxNodeSize   the maximum size of an index node (Branch or Leaf)
     * @throws IOException occurs when there is a problem reading or writing from a store.
     */
    public BLSMTreeMap(Store store, Comparator<K> keyComparator, long maxIndexSize,
                       long maxNodeSize) throws IOException {
        this(store, keyComparator, new ObjectSerializer(), new ObjectSerializer(), maxIndexSize,
                maxNodeSize, -1);
    }

    /**
     * Creates an instance of this bLSM Tree.
     *
     * @param store           the store used to manage the on disk(?) indexes
     * @param keyComparator   the key comparator
     * @param keySerializer   the serializer used for storing keys
     * @param valueSerializer the serializer used for storing values
     * @param maxIndexSize    the maximum size of an index in memory
     * @param maxNodeSize     the maximum size of an index node (Branch or Leaf)
     * @throws IOException occurs when there is a problem reading or writing from a store.
     */
    public BLSMTreeMap(Store store, Comparator<K> keyComparator,
                       Serializer keySerializer, Serializer valueSerializer, long maxIndexSize, long maxNodeSize)
            throws IOException {
        this(store, keyComparator, keySerializer, valueSerializer, maxIndexSize,
                maxNodeSize, -1l);
    }

    /**
     * Creates an instance of this bLSM Tree.
     *
     * @param store           the store used to manage the on disk(?) indexes
     * @param keyComparator   the key comparator
     * @param keySerializer   the serializer used for storing keys
     * @param valueSerializer the serializer used for storing values
     * @param maxIndexSize    the maximum size of an index in memory
     * @param maxNodeSize     the maximum size of an index node (Branch or Leaf)
     * @param pointer         the pointer to the meta data for this tree.
     * @throws IOException occurs when there is a problem reading or writing from a store.
     */
    public BLSMTreeMap(Store store, Comparator<K> keyComparator,
                       Serializer keySerializer, Serializer valueSerializer, long maxIndexSize, long maxNodeSize,
                       long pointer) throws IOException {
        super(keyComparator);
        this.store = store;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.maxNodeSize = maxNodeSize;
        this.pointer = pointer; // FIXME: Load meta data from pointer!
        this.maxIndexSize = maxIndexSize;
        this.memoryTree = createNewMemoryTree();
        // Load meta data from pointer
        loadTreeTables();

        // scheduled executor for merge...
//        mergeExecutor.scheduleAtFixedRate(() -> mergeTrees(), 30, 30, TimeUnit.SECONDS);
    }

    private void loadTreeTables() throws IOException {
        if (pointer < 0) {
            return; // do nothing as it's a new tree
        }
        List<Long> pointers = (List<Long>) store.get(pointer, new ObjectSerializer());
        BTreeMap<K, V>[] tables = new BTreeMap[pointers.size()];
        for (int i = 0; i < pointers.size(); i++) {
            BTreeMap<K, V> tree = treeFromPointer(pointers.get(i).longValue());
            tables[i] = tree;
        }
        // Set the index tables up
        this.tables = tables;
    }

    private void storePagesOnDisk() throws IOException {
        long[] pointers = new long[tables.length];
        for (int i = 0; i < tables.length; i++) {
            pointers[i] = tables[i].getPointer();
        }
        // Add the pages to disk... This is a bit crappy, but it's all we need to make this work! A simple array
        // on disk...
        pointer = store.add(pointers, new ObjectSerializer());
    }

    /**
     * Creates a BTree in memory
     *
     * @return the newly created btree
     * @throws IOException an exception when there is a problem reading or writing.
     */
    private BTreeMap<K, V> createNewMemoryTree() throws IOException {
        // FIXME: Replace storage with builders as new new new is a bit messy
        Store memoryStore = new CachingStore(new DirectStore(new HeapByteBufferVolume("memory", false,
                20, 10 << 20)), 1000);
        // BTree doesn't need to be very large as we're in memory and there is no seek time... So 10
        // entries per node is good enough...
        BTreeMap<K,V> tree = new BTreeMap<>(memoryStore, (Comparator<K>) comparator(),
                keySerializer, valueSerializer, 50);
        memoryTrees.getAndIncrement();
        return tree;
    }

    /**
     * Creates a new disk resident btree instance
     *
     * @return the newly created btree.
     * @throws IOException
     */
    private BTreeMap<K, V> createStoredTree() throws IOException {
        return new BTreeMap<>(store, (Comparator<K>) comparator(), keySerializer, valueSerializer, maxNodeSize);
    }

    /**
     * Creates a disk resident btree instance using a pointer to it's position on the disk.
     *
     * @param pointer the pointer for the btree instance
     * @return the btree from position of pointer
     * @throws IOException if there is a read / write fault...
     */
    private BTreeMap<K, V> treeFromPointer(long pointer) throws IOException {
        return new BTreeMap<>(store, (Comparator<K>) comparator(), keySerializer, valueSerializer, maxNodeSize, pointer);
    }

    private BTreeMap<K, V> findTreeWithKey(K key) throws IOException {
        // FIXME: Replace with bloom filter
        if (treeHasKey(memoryTree, key)) {
            return memoryTree;
        }

        if (tables == null) {
            return null;
        }
        for (BTreeMap<K, V> tree : tables) {
            if (treeHasKey(tree, key)) {
                return tree;
            }
        }
        return null;
    }

    private boolean treeHasKey(BTreeMap<K, V> tree, K key) {
        // FIXME: Add a bloody BLOOM FILTER
        return tree.containsKey(key);
    }

    /**
     * Flushes the memory resident btree to disk! This means that a new empty memory resident btree can take it's place.
     * The flush to disk happens asynchronously, that meaning a promise is created and executed to ensure that it's
     * pushed to disk.
     * <p>
     * Trees that are pushed to disk are checked against existing disk based indexes so that a potential merge can take
     * place asynchronously at a later stage. This is how a bLSM tree should work: All indexes of the same size are
     * merged into bigger, newer indexes asynchronously (so as not to affect normal operation of the bLSM tree).
     * <p>
     * Merging ensures that there are fewer, larger indexes on disk at any one time so as to improve read performance.
     *
     * @throws IOException
     */
    private void flush() throws IOException {
        // Place memory tree on to table until we are done with it...
        BTreeMap<K, V> storedTree = createStoredTree();
        memoryTree.forEach(storedTree::put);

        assert storedTree.sizeLong() == memoryTree.sizeLong() : "Tree sizes do not match";
        // Set tables to new tables
        // create a new table for the btrees
        // Add new index to end of tables
        BTreeMap<K, V>[] newTable = tables == null ? new BTreeMap[1] : Arrays.copyOf(tables, tables.length + 1);
        newTable[newTable.length - 1] = storedTree;
        tables = newTable;

        // Create a new memory tree
        memoryTree = createNewMemoryTree();
    }

    public void merge() {

        // Otherwise, merge the trees asynchronously into a new tree
        final AtomicLong pointer = new AtomicLong(-1);
        final AtomicInteger index = new AtomicInteger(-1);

        // If there are less than 2 tables, then don't do anything
        if (tables.length < 2) {
            return;
        }

        // Get the first and second indexes.
        final BTreeMap<K, V> first = tables[tables.length - 1];
        final BTreeMap<K, V> second = tables[tables.length - 2];

        index.set(tables.length - 2);
        pointer.set(second.getPointer());

        // If the first table is smaller than the second, then do nothing...
        if (first.sizeLong() < second.sizeLong()) {
            return;
        }
        // Add the first tree on top, replacing updated records with newer versions
        second.putAll(first);
        // Once all the records have been written then create a new tables array..
        BTreeMap<K, V>[] newTables = Arrays.copyOf(tables, tables.length - 1);

        lock.writeLock().lock();
        try {
            tables = newTables;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Return the current pointer for this LSM Tree...
     *
     * @return the pointer to the LSM tree...
     */
    public long getPointer() {
        return pointer;
    }

    /**
     * Retrieves a value by key from the nearest btree. This is done with the use of a bloom filter. The
     * method will check each btree when a bloom filter indicates that a record may exist, until that record is found.
     * Each time a bloom filter responds with a negative, the next filter is checked until none are left and the method
     * will return null. If the record is found at index[n], then the value is returned.
     *
     * @param key the key of the entry being looked up.
     * @return the value of the entry or null if not found.
     */
    @Override
    public Entry<K, V> getEntry(K key) {
        lock.readLock().lock();
        try {
            // Find a tree with the key, then return it
            BTreeMap<K, V> tree = findTreeWithKey(key);
            // If there is no key in any of the trees, return null.
            if (tree == null) {
                return null;
            }
            // return the value from the found tree
            return tree.getEntry(key);
        } catch (IOException ex) {
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Entry<K, V> putEntry(K key, V value, boolean replace) {

        lock.writeLock().lock();
        try {
            // Flush if the tree is already at it's maximum size
            if (memoryTree.sizeLong() >= maxIndexSize) {
                try {
                    flush();
                } catch (Throwable ex) {
                    // FIXME: Handle this
                    throw new RuntimeException(ex);
                }
            }

            // Place the entry into the memory tree, we don't need to update meta data, because it's all memory
            // We are not worried about duplicates as the first tree with the key is considered the current version
            return memoryTree.putEntry(key, value, replace);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    protected Iterator<Entry<K, V>> descendingIterator(Comparator<? super K> comparator, K low,
                                                       boolean lowInclusive, K high, boolean highInclusive) {
        return new BLSMTreeDescendingIterator<>(this, (Comparator<K>) comparator, low, lowInclusive, high,
                highInclusive);
    }

    @Override
    protected Iterator<Entry<K, V>> iterator(Comparator<? super K> comparator, K low,
                                             boolean lowInclusive, K high, boolean highInclusive) {
        return new BLSMTreeIterator<>(this, (Comparator<K>) comparator, low, lowInclusive, high, highInclusive);
    }

    @Override
    public boolean remove(Object key, Object value) {
        boolean removed = false;
        // Find all the indexes with the value
        List<BTreeMap<K, V>> indexesWithKey = new ArrayList<>();
        if (treeHasKey(memoryTree, (K) key)) {
            removed = memoryTree.remove(key) != null;
        }

        // Check each index in turn and cull the key from it
        for (BTreeMap<K, V> tree : tables) {
            if (treeHasKey(tree, (K) key)) {
                if (tree.remove(key) != null) {
                    removed = true;
                }
            }
        }
        return removed;
    }

    @Override
    public int size() {
        return (int) Math.min(Integer.MAX_VALUE, sizeLong());
    }

    public long sizeLong() {
        lock.readLock().lock();
        try {
            long size = memoryTree.sizeLong();
            for (BTreeMap<K, V> tree : tables) {
                size += tree.sizeLong();
            }
            return size;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void clear() {
        // errrrrr... DO NOTHING, KNOW NOTHING.. BAD MMMM....K!
        new UnsupportedOperationException("You cannot run clear on a map like this.");
    }

    /**
     * Returns an entry set representing this map and all it's child tree maps.
     * <p>
     * This has the potential to return very large sets of data and caution must be applied.
     *
     * @return the entryset for this map.
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        return new EntrySet<>(this);
    }

    /*
     * We need a tree iterator that iterates through each tree... This is not easy at all...
     *
     * memoryTree.iterator().first();
     *
     * for each tree in tables do tree.first() and add to an array of tables.length the values in order.
     *
     * Cycle through each of the retrieved values and find the first (by comparison) the first value from the iterator
     * of the tree that holds it and set it's current value to that value. Repeat the cycle until the second value is
     * found, then the third, then the fourth, etc.
     *
     */
    private static class BLSMTreeIterator<K, V> implements Iterator<Entry<K, V>> {

        protected Entry<K, V>[] currentEntries;
        protected Iterator<Entry<K, V>>[] iterators;

        protected final Comparator<K> comparator;
        protected final BLSMTreeMap<K, V> map;
        protected final K low, high;
        protected final boolean lowInclusive, highInclusive;

        public BLSMTreeIterator(BLSMTreeMap<K, V> map, Comparator<K> comparator, K low, boolean lowInclusive,
                                K high, boolean highInclusive) {

            this.map = map;
            this.comparator = comparator;
            this.low = low;
            this.high = high;
            this.lowInclusive = lowInclusive;
            this.highInclusive = highInclusive;
            currentEntries = new Entry[map.tables.length + 1];
            iterators = new Iterator[map.tables.length + 1];

            pointToStart();
        }

        protected void pointToStart() {
            setupIteratorAndCurrentEntry(0, map.memoryTree);
            for (int i = map.tables.length; i > 0; i--) {
                setupIteratorAndCurrentEntry(i, map.tables[i - 1]);
            }
        }

        protected void setupIteratorAndCurrentEntry(int position, ConcurrentNavigableMap<K, V> map) {
            if (low != null && high != null) {
                ConcurrentNavigableMap<K, V> tree = map.subMap((K) low, lowInclusive, (K) high, highInclusive);
                iterators[position] = tree.entrySet().iterator();
                currentEntries[position] = iterators[position].next();
            } else if (low == null && high != null) {
                ConcurrentNavigableMap<K, V> tree = map.headMap((K) high, highInclusive);
                iterators[position] = tree.entrySet().iterator();
                currentEntries[position] = iterators[0].next();
            } else if (low != null && high == null) {
                ConcurrentNavigableMap<K, V> tree = map.tailMap((K) low, lowInclusive);
                iterators[position] = tree.entrySet().iterator();
                currentEntries[position] = iterators[0].next();
            } else {
                iterators[position] = map.entrySet().iterator();
                currentEntries[position] = iterators[position].next();
            }
        }

        @Override
        public boolean hasNext() {
            for (Iterator<Entry<K, V>> it : iterators) {
                if (it.hasNext()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Entry<K, V> next() {
            if (!hasNext()) {
                return null;
            }
            // Loop through each of the current entries and their position
            SortedMap<K, Integer> entries = new TreeMap<K, Integer>(comparator);
            for (int i = 0; i < currentEntries.length; i++) {
                if (currentEntries[i] == null) {
                    // If this happens it means that we have no next on this tree.
                    continue;
                }
                entries.put(currentEntries[i].getKey(), i);
            }
            // Nasty but should work fine.
            int entryIndex = entries.entrySet().iterator().hasNext() ? entries.entrySet().iterator().next()
                    .getValue() : -1;
            // Entry index should be greater than -1
            if (entryIndex == -1) {
                return null;
            }
            // Entry should never be null
            Entry<K, V> entry = currentEntries[entryIndex];
            // We need to advance
            currentEntries[entryIndex] = iterators[entryIndex].hasNext() ? iterators[entryIndex].next() : null;
            return entry;
        }
    }

    private static class BLSMTreeDescendingIterator<K, V> extends BLSMTreeIterator<K, V> {


        public BLSMTreeDescendingIterator(BLSMTreeMap<K, V> map, Comparator<K> comparator, K low, boolean lowInclusive,
                                          K high, boolean highInclusive) {
            super(map, comparator, low, lowInclusive, high, highInclusive);
        }

        @Override
        protected void pointToStart() {
            setupIteratorAndCurrentEntry(0, map.memoryTree.descendingMap());
            for (int i = map.tables.length; i > 0; i--) {
                setupIteratorAndCurrentEntry(i, map.tables[i - 1].descendingMap());
            }
        }
    }
}
