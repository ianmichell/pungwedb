/*
 * Copyright (C) 2016 Ian Michell.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pungwe.db.common.collections.btree;

import com.google.common.hash.BloomFilter;
import com.pungwe.db.common.io.memory.Allocator;
import com.pungwe.db.core.error.DatabaseRuntimeException;
import com.pungwe.db.core.io.serializers.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Memory B+Tree. Data is store in leaves, which are broken into nodes when the maximum node size is achieved. Nodes
 * are always set to their maximum capacity in memory. Keys and values are stored separately and fetched by their
 * offset.
 * <p>
 * Branches have BranchEntries with key + left + right.
 * <p>
 * Leaves have KeyEntries with key + Pair (which has child reference and a deleted flag).
 * <p>
 * Nodes will store the key, whilst the value is always a reference, either long
 *
 * @param <K>
 * @param <V>
 */
public class MemoryBTreeMap<K, V> extends AbstractBTreeMap<K, V> {

    /**
     * Bit shift of 20 represents one MB so 1 << 20 is 1MB or 1024 << 20 is 1GB.
     */
    private static final int shift = 20;
    /**
     * Slices of 1MB, this means we may straddle slices if values are too large to fit, but make maintenance much
     * easier, as we trim by MB, not GB....
     */
    private static final int sliceSize = 1 << shift;
    /**
     * Mod mast is the relative position within a slice for data.
     */
    private static final int sliceSizeModMask = sliceSize - 1;
    /**
     * The capacity of the B+Tree in bytes.
     */
    private final long capacity;
    /**
     * Bloom serializer is a key serializer optimized for bloom. This is useful when you want to exclude values from
     * hash code generation... If null on the constructor it's the same value as the keySerializer.
     */
    private final Serializer<K> bloomSerializer;

    /**
     * Transforms keys into raw byte data.
     */
    private final Serializer<K> keySerializer;

    /**
     * Transforms values into raw byte data.
     */
    private final Serializer<V> valueSerializer;

    /**
     * Contains an array of bits to speed up lookups.
     */
    private final BloomFilter<K> bloomFilter;

    /**
     * Current segment offset.
     */
    private final AtomicLong position = new AtomicLong();

    /**
     * The root offset of the top most node.
     */
    private long rootOffset;

    /**
     * Segments of memory used to store serialized data. Maximum size is 100MB per segment.
     */
    private Allocator.AllocatedMemory[] segments = new Allocator.AllocatedMemory[0];

    public MemoryBTreeMap(Comparator<K> keyComparator, Serializer<K> keySerializer, Serializer<V> valueSerializer,
                          int maxNodeSize, long capacity) throws IOException {
        this(keyComparator, keySerializer, keySerializer, valueSerializer, maxNodeSize, capacity);
    }

    /**
     *
     * @param keyComparator the key comparator for sorting keys
     * @param keySerializer the key serializer to convert keys to bytes
     * @param bloomKeySerializer the bloom serializer, for custom bloom filter hashes.
     * @param valueSerializer the value serializer, to convert values into bytes
     * @param maxNodeSize the maximum number of keys per node.
     * @param capacity the total capacity in MB of the B+Tree
     */
    public MemoryBTreeMap(Comparator<K> keyComparator, Serializer<K> keySerializer, Serializer<K> bloomKeySerializer,
                          Serializer<V> valueSerializer, int maxNodeSize, long capacity) throws IOException {
        super(keyComparator, maxNodeSize);
        this.bloomSerializer = bloomKeySerializer;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.capacity = capacity;
        this.bloomFilter = BloomFilter.create((from, into) -> {
            try {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes);
                MemoryBTreeMap.this.bloomSerializer.serialize(out, from);
                into.putBytes(bytes.toByteArray());
            } catch (IOException ex) {
                throw new DatabaseRuntimeException(ex);
            }
        }, maxNodeSize);
        // Root offset....
        rootOffset = writeNode(new Leaf<>(-1L, this.comparator));
    }

    @Override
    protected BTreeEntry<K, V> putEntry(Entry<? extends K, ? extends V> entry) {
        // Find leaf...
        return null;
    }

    @Override
    public BTreeEntry<K, V> getEntry(K key) {
        return null;
    }

    @Override
    protected V removeEntry(K key) {
        return null;
    }

    @Override
    public long sizeLong() {
        return 0;
    }

    @Override
    protected Iterator<Entry<K, V>> iterator(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return null;
    }

    @Override
    protected Iterator<Entry<K, V>> reverseIterator(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return null;
    }

    @Override
    public Iterator<Entry<K, V>> mergeIterator() {
        return null;
    }

    @Override
    protected Iterator<Entry<K, V>> mergeIterator(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return null;
    }

    @Override
    protected AbstractBTreeMap.Node<K, ?> rootNode() {
        return null;
    }

    @Override
    protected BloomFilter<K> bloomFilter() {
        return null;
    }

    @Override
    public Set<BTreeEntry<K, V>> getEntries(Collection<K> keys) {
        return null;
    }

    @Override
    public void clear() {

    }

    @SuppressWarnings("unchecked")
    private Leaf<K> findLeaf(K key) throws IOException {
        Node<K, ?> node = fetchNode(rootOffset);
        while (!Leaf.class.isAssignableFrom(node.getClass())) {
            Map.Entry<Key<K>, ?> entry = node.tailMap(new Key<>(key)).firstEntry();
            // We now have an entry... We need to run a comparison to see if it's greater than or lower than...
            if (comparator.compare(key, entry.getKey() != null ? entry.getKey().getKey() : null) >= 0) {
                // There will always be a pair in the long array...
                long right = ((Map.Entry<K, Long[]>)entry).getValue()[1];
                node = fetchNode(right);
            } else {
                // There will always be a pair in the long array...
                long left = ((Map.Entry<K, Long[]>)entry).getValue()[0];
                node = fetchNode(left);
            }
        }
        return (Leaf<K>)node;
    }

    private Node<K, ?> fetchNode(long offset) throws IOException {
        return new Leaf<>(offset, comparator);
    }

    /**
     * Builds a stored node from a B+Tree node. Serializes each key (and value if it's a leaf) into the memory segments.
     * @param node the node to write to memory.
     *
     * @return the offset of the node.
     *
     * @throws IOException if there is a problem writing the value.
     */
    private long writeNode(Node<K, ?> node) throws IOException {
        boolean branch = Branch.class.isAssignableFrom(node.getClass());
        long[] keys = new long[maxKeysPerNode];
        long[] values = new long[branch ? maxKeysPerNode + 1 : maxKeysPerNode];
        // If offset is -1, then it's new, so get the current position and use that....
        long offset = node.offset;
        int i = 0;
        // We want to save memory and reuse keys... This is pretty important
        for (Map.Entry<Key<K>, ?> entry : node.entrySet()) {
            // Store the key and push it to the
            keys[i] = storeKey(entry.getKey());
            // If it's a branch, we need to add the right hand child.
            if (branch) {
                // Left hand is always put on the left.
                values[i++] = ((Long[])entry.getValue())[0];
                // Right hand is always on the right.
                values[i++] = ((Long[])entry.getValue())[1];
            } else {
                // Otherwise store the reference to the value!
                values[i++] = (Long)entry.getValue();
            }
        }
        // Set the remaining elements to -1.
        while (i < keys.length) {
            keys[i] = -1;
            if (branch) {
                values[++i] = -1;
            } else {
                values[i++] = -1;
            }
        }
        // Create a stored node...
        StoredNode storedNode = new StoredNode(branch, keys, values);
        // Write the stored node to memory...
        return writeStoredNode(offset, storedNode);
    }

    /**
     *
     * @param offset
     * @param node
     * @return
     */
    private long writeStoredNode(long offset, StoredNode node) throws IOException {
        // Get the byte value of the node.
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(bytes);
        // Write the branch boolean.
        dataOut.writeBoolean(node.branch);
        // Write the keys
        for (long key : node.keys) {
            dataOut.writeLong(key);
        }
        for (long value : node.values) {
            dataOut.writeLong(value);
        }
        byte[] data = bytes.toByteArray();
        // get the size of the data...
        int size = data.length;
        // Calculate offset.
        if (offset < 0) {
            offset = position.getAndAdd(size);
        }

        return offset;
    }

    private long storeKey(Key<K> key) throws IOException {
        return -1;
    }

    private long storeValue(V value) throws IOException {
        return -1;
    }

    private void writeData(long offset, byte[] bytes) throws IOException {
        int size = bytes.length;
        int written = 0;
        while (written < size) {
            // Returns the index of the segment to write to.
            int pos = (int) ((offset + written) >>> shift);
            // Get the segment at pos
            Allocator.AllocatedMemory memory = segments[pos];
            // Get data output for the memory
            DataOutput out = memory.getDataOutput((offset + written) & sliceSizeModMask);
            // Return the most appropriate offset.
            int sizeToWrite = (int) Math.min((size - written), memory.remaining());
            // Write data to first segment
            out.write(bytes, 0, sizeToWrite);
        }
    }

    private static class Key<K> {
        private final K key;
        private final long offset;

        public Key(K key) {
            this(key, -1);
        }

        public Key(K key, long offset) {
            this.key = key;
            this.offset = offset;
        }

        public K getKey() {
            return key;
        }

        public long getOffset() {
            return offset;
        }
    }

    private static class Value<V> {
        private final long offset;
        private final V value;
        private final boolean deleted;

        public Value(long offset, V value, boolean deleted) {
            this.offset = offset;
            this.value = value;
            this.deleted = deleted;
        }

        public long getOffset() {
            return offset;
        }

        public V getValue() {
            return value;
        }

        public boolean isDeleted() {
            return deleted;
        }
    }

    private static class Node<K, V> extends ConcurrentSkipListMap<Key<K>, V> {

        private final long offset;

        private Node(long offset, final Comparator<K> comparator) {
            super((o1, o2) -> comparator.compare(o1 == null ? null : o1.getKey(), o2 == null ? null :  o2.getKey()));
            this.offset = offset;
        }
    }

    private static class Branch<K> extends Node<K, Long[]> {

        private Branch(long offset, Comparator<K> comparator) {
            super(offset, comparator);
        }
    }

    private static class Leaf<K> extends Node<K, Long> {

        private Leaf(long offset, Comparator<K> comparator) {
            super(offset, comparator);
        }
    }

    private static class StoredNode {
        private final boolean branch;
        private final long[] keys;
        private final long[] values;

        public StoredNode(final boolean branch, final long[] keys, final long[] values) {
            this.branch = branch;
            this.keys = keys;
            this.values = values;
        }
    }
}
