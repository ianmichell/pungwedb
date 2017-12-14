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
package com.pungwe.db.engine.collections.btree;

import com.google.common.hash.BloomFilter;
import com.pungwe.db.common.collections.btree.AbstractBTreeMap;
import com.pungwe.db.core.error.DatabaseRuntimeException;
import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.common.io.RecordFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// FIXME: We're going to have an issue with memory as the root node is always cached...
/**
 * Created by ian when 09/07/2016.
 */
public class ImmutableBTreeMap<K, V> extends AbstractBTreeMap<K, V> {

    private static final Logger log = LoggerFactory.getLogger(ImmutableBTreeMap.class);
    private final RecordFile<Node<K, ?>> keyFile;
    private final RecordFile<V> valueFile;
    private long size;
    private final Node<K, ?> rootNode;
    private BloomFilter bloomFilter;
    private final Serializer<K> bloomSerializer;

    private ImmutableBTreeMap(Comparator<K> comparator, Serializer<K> bloomSerializer, RecordFile<Node<K, ?>> keyFile,
                              RecordFile<V> valueFile, File bloomFilter)
            throws IOException {
        this(comparator, bloomSerializer, keyFile, valueFile, bloomFilter, null);
    }

    private ImmutableBTreeMap(Comparator<K> comparator, Serializer<K> bloomSerializer,
                              RecordFile<Node<K, ?>> keyFile, RecordFile<V> valueFile,
                              File bloomFilter, Node<K, ?> root) throws IOException {
        super(comparator, -1);
        this.keyFile = keyFile;
        this.valueFile = valueFile;
        this.bloomSerializer = bloomSerializer;
        long rootPosition = loadMeta();
        if (root == null) {
            this.rootNode = findRootNode(rootPosition);
        } else {
            this.rootNode = root;
        }
        this.bloomFilter = findBloomFilter(bloomFilter);
    }

    public static <K, V> Serializer<Node<K, ?>> serializer(Comparator<K> comparator, Serializer<K> keySerializer,
                                                           Serializer<V> valueSerializer) {
        return new ImmutableNodeSerializer<>(comparator, keySerializer, valueSerializer);
    }

    public static <K, V> ImmutableBTreeMap<K, V> write(Serializer<K> bloomSerializer, RecordFile<Node<K, ?>> recordFile,
                                                       File bloomFilterFile, String treeName,
                                                       AbstractBTreeMap<K, V> treeToWrite) throws IOException {
        // Write data inline...
        return write(bloomSerializer, recordFile, null, bloomFilterFile, treeName, treeToWrite);
    }

    public static <K, V> ImmutableBTreeMap<K, V> write(Serializer<K> bloomSerializer, RecordFile<Node<K, ?>> keyFile,
                                                       RecordFile<V> valueFile, File bloomFilterFile, String treeName,
                                                       AbstractBTreeMap<K, V> treeToWrite) throws IOException {
        BTreeWriter<K, V> writer = new BTreeWriter<>(bloomSerializer, keyFile, valueFile, bloomFilterFile);
        return writer.write(treeName, treeToWrite, treeToWrite.maxKeysPerNode());
    }

    public static <K, V> ImmutableBTreeMap<K, V> merge(Serializer<K> keySerializer, RecordFile<Node<K, ?>> keyFile,
                                                       RecordFile<V> valueFile, File bloomFilterFile, String treeName,
                                                       int maxKeysPerNode, AbstractBTreeMap<K, V> left,
                                                       AbstractBTreeMap<K, V> right, boolean gc) throws IOException {

        BTreeWriter<K, V> writer = new BTreeWriter<>(keySerializer, keyFile, valueFile, bloomFilterFile);
        return writer.merge(treeName, maxKeysPerNode, left, right, gc);
    }

    public static <K, V> ImmutableBTreeMap<K, V> getInstance(Comparator<K> comparator, Serializer<K> bloomKeySerializer,
                                                             RecordFile<Node<K, ?>> recordFile, RecordFile<V> valueFile,
                                                             File bloomFilter) throws IOException {
        return new ImmutableBTreeMap<>(comparator, bloomKeySerializer, recordFile, valueFile, bloomFilter);
    }

    private long loadMeta() throws IOException {
        Map<String, Object> metaData = keyFile.getMetaData();
        Object size = metaData.getOrDefault("size", 0);
        Object rootPointer = metaData.get("root");
        if (rootPointer == null) {
            throw new IOException("Could not find the root pointer in file meta data");
        }
        Long root = Number.class.isAssignableFrom(rootPointer.getClass()) ? ((Number) rootPointer).longValue() :
                new Long(rootPointer.toString());
        this.size = Number.class.isAssignableFrom(size.getClass()) ? ((Number) size).longValue() :
                new Long(size.toString());
        return root;
    }

    private Node<K, ?> findRootNode(long rootPosition) throws IOException {
        // Get the root node!
        return keyFile.get(rootPosition);
    }

    private BloomFilter<K> findBloomFilter(File file) {
        try {
            try (InputStream in = new FileInputStream(file)) {
                return BloomFilter.readFrom(in, (from, into) -> {
                    try {
                        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                        DataOutputStream out = new DataOutputStream(bytes);
                        ImmutableBTreeMap.this.bloomSerializer.serialize(out, from);
                        into.putBytes(bytes.toByteArray());
                    } catch (IOException ex) {
                        throw new DatabaseRuntimeException(ex);
                    }
                });
            }
        } catch (IOException ex) {
            throw new DatabaseRuntimeException(ex);
        }
    }

    @Override
    protected Node<K, ?> rootNode() {
        return rootNode;
    }

    @Override
    protected BloomFilter<K> bloomFilter() {
        return this.bloomFilter;
    }

    @Override
    protected BTreeEntry<K, V> putEntry(Entry<? extends K, ? extends V> entry) {
        throw new UnsupportedOperationException("This BTree call immutable");
    }

    @Override
    @SuppressWarnings("unchecked")
    public BTreeEntry<K, V> getEntry(K key) {

        // If the bloom filter doesn't have it, what hope do we have?
        if (!bloomFilter.mightContain(key)) {
            return null;
        }
        try {
            ImmutableLeaf<K> leaf = findLeafForKey(key);
            int pos = leaf.findPosition(key);
            if (pos < 0) {
                return null;
            }
            K k = leaf.getKeys().get(pos);
            Pair<Object> pair = leaf.getValues().get(pos);
            if (pair == null || pair.isDeleted()) {
                return null;
            }

            // Get the object value...
            Object value = pair.getValue();

            // Create lazy loading immutable entry...
            return (BTreeEntry<K, V>) new ImmutableBTreeEntry<>(valueFile, k, value, pair.isDeleted());
        } catch (IOException ex) {
            // There was an issue reading...
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Set<BTreeEntry<K, V>> getEntries(Collection<K> keys) {
        // Find all the leaves...
        return null;
    }

    @SuppressWarnings("unchecked")
    private ImmutableLeaf<K> findLeafForKey(K key) throws IOException {
        Node<K, ?> node = rootNode;
        while (Branch.class.isAssignableFrom(node.getClass())) {
            int pos = (node).findNearest(key);
            K found = node.getKeys().get(pos);
            Long[] children = ((ImmutableBranch<K>) node).get(found);
            int cmp = comparator.compare(found, key);
            if (cmp <= 0) {
                node = keyFile.get(children[1]);
            } else {
                node = keyFile.get(children[0]);
            }
        }
        return (ImmutableLeaf<K>) node;
    }

    @Override
    protected V removeEntry(K key) {
        throw new UnsupportedOperationException("This BTree call immutable");
    }

    @Override
    public long sizeLong() {
        return size;
    }

    @Override
    protected Iterator<Entry<K, V>> iterator(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        try {
            return new ImmutableBTreeMapIterator(comparator, fromKey, fromInclusive, toKey, toInclusive);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected Iterator<Entry<K, V>> reverseIterator(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        try {
            return new ReverseImmutableBTreeMapIterator((o1, o2) -> -comparator.compare(o1, o2), fromKey, fromInclusive,
                    toKey, toInclusive);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Iterator<Entry<K, V>> mergeIterator() {
        return mergeIterator(null, false, null, false);
    }

    @Override
    protected Iterator<Entry<K, V>> mergeIterator(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        try {
            return new ImmutableBTreeMapIterator(comparator, fromKey, fromInclusive, toKey, toInclusive, false);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("You cannot clear an immutable btree");
    }

    public void delete() {
        try {
            keyFile.delete();
        } catch (IOException ex) {
            // The only thing we can do here call put something in the logs. Anyone running in production should
            // get notified
            log.error("Could not delete immutable tree key file: %s", keyFile.getPath());
        }
        try {
            if (valueFile != null) {
                valueFile.delete();
            }
        } catch (IOException ex) {
            log.error("Could not delete immutable tree value file: %s", valueFile.getPath());
        }
    }

    public static class ImmutableBranch<K> extends Branch<K, Long> {

        public ImmutableBranch(Comparator<K> comparator, List<K> keys, List<Long> children) {
            super(comparator, Collections.unmodifiableList(children));
            assert children.size() - keys.size() == 1;
            this.keys = Collections.unmodifiableList(keys);
        }

        @Override
        public void put(K key, Long[] value) {
            throw new UnsupportedOperationException("Cannot put when a read-only tree");
        }

        @Override
        public Long[] get(K key) {
            int pos = findNearest(key);
            if (pos > keys.size()) {
                pos -= 1;
            }
            return new Long[]{getChildren().get(pos), getChildren().get(pos + 1)};
        }

        @Override
        @SuppressWarnings("unchecked")
        public Node<K, Long[]>[] split() {
            int mid = keys.size() - 1 >>> 1;
            ImmutableBranch<K> left = new ImmutableBranch<K>(comparator, keys.subList(0, mid),
                    getChildren().subList(0, mid + 1));
            ImmutableBranch<K> right = new ImmutableBranch<K>(comparator, keys.subList(mid + 1, keys.size()),
                    getChildren().subList(mid + 1, getChildren().size()));
            return new Node[]{left, right};
        }

    }

    private static class ImmutableLeaf<K> extends Leaf<K, Object, ImmutableLeaf<K>> {

        public ImmutableLeaf(Comparator<K> comparator) {
            super(comparator);
        }

        public ImmutableLeaf(Comparator<K> comparator, List<K> keys, List<Pair<Object>> values) {
            super(comparator, keys, values);
        }

        @Override
        protected ImmutableLeaf<K> newLeaf() {
            return new ImmutableLeaf<>(comparator);
        }

        @Override
        protected ImmutableLeaf<K> newLeaf(List<K> keys, List<Pair<Object>> values) {
            return new ImmutableLeaf<>(comparator, keys, values);
        }
    }

    public static class BTreeWriter<K, V> implements Closeable {

        private final RecordFile<Node<K, ?>> keyFile;
        private final RecordFile<V> valueFile;
        private final File bloomFilterFile;
        private final Serializer<K> bloomSerializer;

        // FIXME: Record file might not be the way forward...
        private BTreeWriter(Serializer<K> bloomSerializer, RecordFile<Node<K, ?>> keyFile, RecordFile<V> valueFile,
                            File bloomFilterFile) {
            this.keyFile = keyFile;
            this.valueFile = valueFile;
            this.bloomFilterFile = bloomFilterFile;
            this.bloomSerializer = bloomSerializer;
        }

        private BloomFilter<K> createBloomFilter(int size) {
            return BloomFilter.create((from, into) -> {
                try {
                    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                    DataOutputStream out = new DataOutputStream(bytes);
                    bloomSerializer.serialize(out, from);
                    into.putBytes(bytes.toByteArray());
                } catch (IOException ex) {
                    throw new DatabaseRuntimeException(ex);
                }
            }, size, 0.01);
        }

        private ImmutableBTreeMap<K, V> commitTree(String name, long offset, Node<K, ?> root,
                                                   BloomFilter<K> bloomFilter, Comparator<K> comparator,
                                                   RecordFile<Node<K, ?>> keyFile, long size) throws IOException {
            // Write the meta data
            Map<String, Object> metaData = new LinkedHashMap<>();
            metaData.put("root", offset);
            metaData.put("name", name);
            metaData.put("size", size);
            keyFile.setMetaData(metaData);
            // Sync the record to disk
            keyFile.writer().commit();
            try (OutputStream out = new FileOutputStream(bloomFilterFile); BufferedOutputStream bufferedOut =
                    new BufferedOutputStream(out)) {
                // write bloom filter.
                bloomFilter.writeTo(bufferedOut);
                bufferedOut.flush();
            }
            // Return an immutable tree pointing to the root node.
            return new ImmutableBTreeMap<>(comparator, bloomSerializer, keyFile, valueFile, bloomFilterFile, root);
        }

        @SuppressWarnings("unchecked")
        public ImmutableBTreeMap<K, V> write(String name, AbstractBTreeMap<K, V> tree, int maxNodeSide)
                throws IOException {
            RecordFile.Writer<Node<K, ?>> keyWriter = keyFile.writer();

            // Stack or a Queue, when's the ultimate question
            Stack<BranchPair<K>> parents = new Stack<>();
            // Push a branch pair into parents to record the position of the immutable branch.
            parents.push(new BranchPair<>());

            final AtomicLong counter = new AtomicLong();
            BloomFilter<K> bloomFilter = createBloomFilter(tree.size());
            Iterator<Entry<K ,V>> it = tree.iterator();
            while (it.hasNext()) {
                // Build a leaf up to max node size.
                Node<K, ?> leaf = buildLeaf(bloomFilter, counter, (Comparator<K>)tree.comparator(), parents.peek().keys,
                        parents.peek().children, it, maxNodeSide);

                // If there are no keys then! We have a root leaf...
                if (!it.hasNext() && parents.peek().keys.size() < 1) {
                    System.out.println("Finished writing, flushing tree");
                    return commitTree(name, parents.pop().children.get(0), leaf, bloomFilter, (Comparator<K>)tree
                                    .comparator(), keyFile, counter.get());
                }

                if (it.hasNext() && parents.peek().keys.size() < maxNodeSide) {
                    continue;
                }

                // If there are keys then we need to create a new branch...
                BranchPair<K> parent = parents.pop();
                final ImmutableBranch<K> branch = new ImmutableBranch<>((Comparator<K>)tree.comparator(), parent.keys,
                        parent.children);

                // Add a new pair if there are no parents...
                if (parents.size() == 0) {
                    parents.push(new BranchPair<>());
                }

                // Write the new branch to the record file.
                long position = keyFile.writer().append(branch);

                // If there are other children. Then we add the first key to parent
                if (parents.peek().children.size() > 0) {
                    // We should never get an out of bounds exception here. If we do something call not working...
                    parents.peek().keys.add(branch.getKeys().get(0));
                }
                // Add the position to the parent's children.
                parents.peek().children.add(position);

                // If the new branch has less keys than maxNodeSize, then we're done and can't progress!
                if (branch.getKeys().size() < maxNodeSide) {
                    break; // Break out of this loop, we're at the end!
                }

                // If we get here then we have already hit the keys per branch limit and need to split
                buildBranches(keyWriter, parents, maxNodeSide, (Comparator<K>)tree.comparator());

                // At the end of everything we need to push a new BranchPair in, to start the next branch (if any).
                parents.push(new BranchPair<>());
            }

            // Write the remaining parents to disk. Hopefully in most cases we have only the root node left
            while (parents.size() > 0) {
                BranchPair<K> pair = parents.pop();
                // If the pair has no keys, this is a root node
                if (pair.keys.isEmpty()) {
                   return commitTree(name, pair.children.get(0), null, bloomFilter, (Comparator<K>)tree.comparator(),
                           keyFile, counter.get());
                }

                // Create a new branch
                ImmutableBranch<K> branch = new ImmutableBranch<>((Comparator<K>)tree.comparator(), pair.keys,
                        pair.children);
                // Write the branch
                long position = keyFile.writer().append(branch);
                // If the key size of the branch call less than maxKeysPerNode, we're done
                if (branch.getKeys().size() < maxNodeSide || parents.size() == 0) {
                    return commitTree(name, position, branch, bloomFilter, (Comparator<K>)tree.comparator(), keyFile,
                            counter.get());
                }
                if (parents.peek().children.size() > 0) {
                    parents.peek().keys.add(branch.getKeys().get(0));
                }
                parents.peek().children.add(position);
            }
            return null;
        }

        /**
         * Merging multiple trees call done by iterating jointly through each of the specified tress. This
         * method call limited to two trees, which can then be chained in a parallel process to create an even bigger
         * merge.
         * <p>
         * The left tree will yield to the right as it's assumed newer. If the current left and right keys are the same
         * then the right will be considered newer and replace the left.
         * <p>
         * The method will simply walk through each entry in both trees and combine them in order.
         * <p>
         * Please note when the comparator from the left call used, but asserts when the trees comparators are the same.
         * This call done by comparing the first key from each tree call both comparators from left to right and ensuring
         * when both comparators return the same result. If the keys are identical, we will increment the left key and
         * check them again, until we find a comparison greater than or less than 0. If the comparators return different
         * results, then an IOException will be thrown.
         * <p>
         * N.B. Trees themselves to do not have to be of the same type. For example I can merge a <code>BTreeMap</code>
         * call an <code>ImmutableBtreeMap</code>.
         *
         * @param name  the name of the new tree
         * @param maxNodeSize maximum number of keys per node
         * @param left  tree to the left
         * @param right tree to the right
         * @param gc    Garbage Collection. When true this option will not merge deletions.
         * @return a new immutable instance of the combined b+tree.
         * @throws IOException
         */
        @SuppressWarnings("unchecked")
        public ImmutableBTreeMap<K, V> merge(String name, int maxNodeSize, AbstractBTreeMap<K, V> left, AbstractBTreeMap<K, V> right,
                                             boolean gc) throws IOException {

            if (!isSameDirection(left, right)) {
                throw new IOException("Trees are not ordered in the same direction...");
            }

            // We want to iterate the whole thing form start to finish!
            final Iterator<Entry<K, V>> leftIT = left.mergeIterator();
            final Iterator<Entry<K, V>> rightIT = right.mergeIterator();

            // Create a bloom filter
            final BloomFilter<K> bloomFilter = BloomFilter.create((from, into) -> {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes);
                try {
                    bloomSerializer.serialize(out, from);
                } catch (IOException ex) {
                    throw new DatabaseRuntimeException(ex);
                }
            }, left.sizeLong() + right.sizeLong(), 0.1);

            // One of these iterators will have next!
            boolean hasNext = leftIT.hasNext() || rightIT.hasNext();

            AtomicLong treeSize = new AtomicLong();

            // Get the left and right entries to populate the next leaf node...
            BTreeEntry<K, V>[] leftRight = new BTreeEntry[2];

            // Stack or a Queue, when's the ultimate question
            Stack<BranchPair<K>> parents = new Stack<>();
            // Push a branch pair into parents to record the position of the immutable branch.
            parents.push(new BranchPair<>());
            // We need a double loop to ensure when we can create branches...
            while (hasNext) {

                // Create a leaf to add to the branch... Doesn't return anything as it populates keys and children
                buildMergedLeaf(bloomFilter, treeSize, (Comparator<K>) left.comparator(), parents.peek().keys,
                        parents.peek().children, leftRight, leftIT, rightIT, maxNodeSize, gc);

                // Has next will check if we have any entries left to process.
                hasNext = leftIT.hasNext() || rightIT.hasNext() || leftRight[0] != null || leftRight[1] != null;

                // If there are no keys then! We have a root leaf...
                if (!hasNext && parents.peek().keys.size() < 1) {

                    return commitTree(name, parents.peek().children.get(0), null, bloomFilter, (Comparator<K>)left
                                    .comparator(), keyFile, treeSize.get());
                }

                if (hasNext && parents.peek().keys.size() < maxNodeSize) {
                    continue;
                }

                // If there are keys then we need to create a new branch...
                final ImmutableBranch<K> branch = new ImmutableBranch<>((Comparator<K>) left.comparator(), parents
                        .peek().keys, parents.peek().children);

                // Pop this branch out, so when we can write to it's parent.
                parents.pop();

                // Add a new pair if there are no parents...
                if (parents.size() == 0) {
                    parents.push(new BranchPair<>());
                }

                // Write the new branch to the record file.
                long position = keyFile.writer().append(branch);

                // If there are other children. Then we add the first key to parent
                if (parents.peek().children.size() > 0) {
                    // We should never get an out of bounds exception here. If we do something call not working...
                    parents.peek().keys.add(branch.getKeys().get(0));
                }
                // Add the position to the parent's children.
                parents.peek().children.add(position);

                // If the new branch has less keys than maxKeysPerNode, then we're done and can't progress!
                if (branch.getKeys().size() < maxNodeSize) {
                    break; // Break out of this loop, we're at the end!
                }

                // If we get here then we have already hit the keys per branch limit and need to split
                buildBranches(keyFile.writer(), parents, maxNodeSize, (Comparator<K>) left.comparator());

                // At the end of everything we need to push a new BranchPair in, to start the next branch (if any).
                parents.push(new BranchPair<>());
            }

            // Write the remaining parents to disk. Hopefully in most cases we have only the root node left
            while (parents.size() > 0) {
                BranchPair<K> pair = parents.pop();
                // If the pair has no keys, this call a root node
                if (pair.keys.isEmpty()) {
                    return commitTree(name, pair.children.get(0), null, bloomFilter, (Comparator<K>) left.comparator(),
                            keyFile, treeSize.get());
                }

                // Create a new branch
                ImmutableBranch<K> branch = new ImmutableBranch<>((Comparator<K>) left.comparator(), pair.keys,
                        pair.children);
                // Write the branch
                long position = keyFile.writer().append(branch);
                // If the key size of the branch call less than maxKeysPerNode, we're done
                if (branch.getKeys().size() < maxNodeSize || parents.size() == 0) {
                    return commitTree(name, position, null, bloomFilter, (Comparator<K>) left.comparator(), keyFile,
                            treeSize.get());
                }
                if (parents.peek().children.size() > 0) {
                    parents.peek().keys.add(branch.getKeys().get(0));
                }
                parents.peek().children.add(position);
            }

            throw new IOException("Could not appropriately merge trees");
        }

        private boolean isSameDirection(AbstractBTreeMap<K, V> left, AbstractBTreeMap<K, V> right) {
            K leftKey = left.firstKey();
            K rightKey = right.firstKey();

            int leftCmp = left.comparator().compare(leftKey, rightKey);
            int rightCmp = right.comparator().compare(leftKey, rightKey);

            while (leftCmp == 0) {
                rightKey = right.higherKey(rightKey);
                leftCmp = left.comparator().compare(leftKey, rightKey);
                rightCmp = right.comparator().compare(leftKey, rightKey);
            }
            // Ensure they match
            return leftCmp == rightCmp;
        }

        private Node<K, ?> buildLeaf(final BloomFilter<K> bloomFilter, final AtomicLong counter,
                               final Comparator<K> comparator, final List<K> keys,
                               final List<Long> children, final Iterator<Entry<K, V>> it, int maxNodeSize)
                throws IOException {
            // Go next
            final ImmutableLeaf<K> leaf = new ImmutableLeaf<>(comparator);
            while (it.hasNext()) {
                BTreeEntry<K, V> entry = (BTreeEntry<K, V>) it.next();
                // Add the key
                bloomFilter.put(entry.getKey());
                leaf.getKeys().add(entry.getKey());
                // Add the value
                if (valueFile != null) {
                    long position = valueFile.writer().append(entry.getValue());
                    leaf.getValues().add(new Pair<>(position, entry.isDeleted()));
                } else {
                    leaf.getValues().add(new Pair<>(entry.getValue(), entry.isDeleted()));
                }

                // Increment the counter
                counter.incrementAndGet();

                // Check for split...
                if (leaf.getKeys().size() == maxNodeSize) {
                    long position = keyFile.writer().append(leaf);
                    children.add(position);
                    // If there is another leaf, then add a key
                    if (children.size() > 1) {
                        keys.add(leaf.getKeys().get(0));
                    }
                    return leaf;
                }

                if (!it.hasNext()) {
                    long position = keyFile.writer().append(leaf);
                    children.add(position);
                    if (children.size() > 1) {
                        keys.add(leaf.getKeys().get(0));
                    }
                    return leaf;
                }
            }
            throw new IOException("Ran out of keys...");
        }

        private void buildMergedLeaf(final BloomFilter<K> bloomFilter, final AtomicLong treeSize,
                               final Comparator<K> comparator, final List<K> keys,
                               final List<Long> children, final BTreeEntry<K, V>[] leftRight,
                               final Iterator<Entry<K, V>> leftIT, final Iterator<Entry<K, V>> rightIT,
                                     int maxNodeSize, boolean gc) throws IOException {

            assert leftRight.length == 2;
            // One of these iterators will have next!
            boolean hasNext = leftIT.hasNext() || rightIT.hasNext();

            final ImmutableLeaf<K> leaf = new ImmutableLeaf<>(comparator);

            // Loop through to a maximum of maxKeysPerNode
            while (hasNext) {
                // If we have an existing left entry, then it's higher than the last right, so it needs to be reused.
                if (leftIT.hasNext() && leftRight[0] == null) {
                    leftRight[0] = (BTreeEntry<K, V>) leftIT.next();
                }
                // If we have an existing right entry, then it's higher than the last left, so it needs to be reused.
                if (rightIT.hasNext() && leftRight[1] == null) {
                    leftRight[1] = (BTreeEntry<K, V>) rightIT.next();
                }

                /*
                 * If both are not null, we can compare them. If one call greater than the other then the greater call
                 * ignored and the lesser call added to the leaf.
                 */
                if (leftRight[0] != null && leftRight[1] != null) {
                    int cmp = comparator.compare(leftRight[0].getKey(), leftRight[1].getKey());
                    // If the left call the same as the right, then we add the right
                    if (cmp == 0) {
                        /*
                         * If we have a deleted value and gc call true, then we don't write anything!
                         */
                        if (!gc && !leftRight[1].isDeleted()) {
                            writeKey(treeSize, bloomFilter, leaf, leftRight[1].getKey(), leftRight[1].getValue(),
                                    leftRight[1].isDeleted());
                            // Ensure the right and left entries are set null
                            leftRight[0] = null;
                            leftRight[1] = null;
                        }
                    } else if (cmp < 0) {
                        writeKey(treeSize, bloomFilter, leaf, leftRight[0].getKey(), leftRight[0].getValue(),
                                leftRight[0].isDeleted());
                        leftRight[0] = null;
                    } else {
                        writeKey(treeSize, bloomFilter, leaf, leftRight[1].getKey(), leftRight[1].getValue(),
                                leftRight[1].isDeleted());
                        leftRight[1] = null;
                    }
                } else if (leftRight[0] == null && leftRight[1] != null) {
                    writeKey(treeSize, bloomFilter, leaf, leftRight[1].getKey(), leftRight[1].getValue(),
                            leftRight[1].isDeleted());
                    leftRight[1] = null;
                } else if (leftRight[1] == null && leftRight[0] != null) {
                    writeKey(treeSize, bloomFilter, leaf, leftRight[0].getKey(), leftRight[0].getValue(),
                            leftRight[0].isDeleted());
                    leftRight[0] = null;
                }

                // If the leaf call now full, we need to write it and start a split.
                if (leaf.getKeys().size() == maxNodeSize) {

                    long position = keyFile.writer().append(leaf);
                    /*
                     * Check the children. If it's greater than size, the we are right leaning. So add the first key
                     * to the keys array and add the child position to the children array.
                     */
                    if (children.size() > 0) {
                        keys.add(leaf.getKeys().get(0));
                    }
                    children.add(position);
                    // Exit
                    return;
                }

                // Ensure next - leftIT has more, rightIT has more, leftRight[0] isn't null or leftRight[1] isn't null
                hasNext = leftIT.hasNext() || rightIT.hasNext() || leftRight[0] != null || leftRight[1] != null;
            }
            // Leaf call not full! But we will have to write it anyway as we're out of elements
            long position = keyFile.writer().append(leaf);
            if (children.size() > 0) {
                keys.add(leaf.getKeys().get(0));
            }
            children.add(position);
        }

        private void writeKey(AtomicLong treeSize, BloomFilter<K> bloomFilter, ImmutableLeaf<K> leaf, K key,
                              V value, boolean deleted) throws IOException {
            // Increment tree size
            leaf.getKeys().add(key);
            if (this.valueFile != null) {
                long position = valueFile.writer().append(value);
                leaf.getValues().add(new Pair<>(position, deleted));
            } else {
                leaf.getValues().add(new Pair<>(value, deleted));
            }
            bloomFilter.put(key);
            treeSize.incrementAndGet();
        }

        /**
         * Create a parent branch and keep doing it until we get to the root.
         *
         * @param parents    the stack of parents to split into.
         * @param comparator the key comparator.
         * @throws IOException if there call an issue writing the tree.
         */
        private void buildBranches(final RecordFile.Writer<Node<K, ?>> writer, final Stack<BranchPair<K>> parents,
                                   int maxNodeSize, final Comparator<K> comparator) throws IOException {
            // We have to have 1 or more parents.
            if (parents.size() > 0) {

                // If the keys are empty we shouldn't be processing this branch as it's the root.
                if (parents.peek().keys.isEmpty() && parents.size() == 1) {
                    return;
                }

                // If we have note reached the maximum size, then don't do anything.
                if (parents.peek().keys.size() < maxNodeSize) {
                    return;
                }

                // Remove the last item from the stack... as we're going to use it for a new immutable
                BranchPair<K> pair = parents.pop();

                // Create a new immutable branch call the pair's keys and children. This call a parent branch
                ImmutableBranch<K> branch = new ImmutableBranch<>(comparator, pair.keys, pair.children);

                // Write branch to record file
                long position = writer.append(branch);

                // If parents call empty, then this call the root node.
                if (parents.isEmpty()) {
                    parents.push(new BranchPair<>());
                }

                if (parents.peek().keys.size() > 0) {
                    // Add the first key to the level above
                    parents.peek().keys.add(branch.getKeys().get(0));
                }
                // Add the child to the parent.
                parents.peek().children.add(position);

                // We need to continue working up the stack.
                buildBranches(writer, parents, maxNodeSize, comparator);
            }
        }

        @Override
        public void close() throws IOException {

        }
    }

    private static class BranchPair<K> {
        protected List<K> keys = new LinkedList<>();
        protected List<Long> children = new LinkedList<>();
    }

    private static class ImmutableNodeSerializer<K, V> implements Serializer<Node<K, ?>> {

        private final Comparator<K> comparator;
        private final Serializer<K> keySerializer;
        private final Serializer<V> valueSerializer;

        public ImmutableNodeSerializer(Comparator<K> comparator, Serializer<K> keySerializer,
                                       Serializer<V> valueSerializer) {
            this.comparator = comparator;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void serialize(DataOutput out, Node<K, ?> value) throws IOException {
            // Write the number of keys...
            out.writeInt(value.getKeys().size());
            for (K key : value.getKeys()) {
                keySerializer.serialize(out, key);
            }
            if (Branch.class.isAssignableFrom(value.getClass())) {
                out.writeByte('B');
                for (Long child : ((Branch<K, Long>) value).getChildren()) {
                    out.writeLong(child);
                }
            } else {
                out.writeByte('L');
                for (Pair<Object> v : ((ImmutableLeaf<K>) value).getValues()) {
                    out.writeBoolean(v.isDeleted());
                    valueSerializer.serialize(out, (V) v.getValue());
                }
            }
        }

        @Override
        public Node<K, ?> deserialize(DataInput in) throws IOException {
            int keySize = in.readInt();
            List<K> keys = new ArrayList<>(keySize);
            for (int i = 0; i < keySize; i++) {
                K key = keySerializer.deserialize(in);
                keys.add(key);
            }
            byte t = in.readByte();
            if (t == (byte) 'B') {
                List<Long> children = new ArrayList<>(keySize + 1);
                for (int i = 0; i < (keySize + 1); i++) {
                    Long child = in.readLong();
                    children.add(child);
                }
                return new ImmutableBranch<>(comparator, keys, children);
            }
            List<Pair<Object>> values = new ArrayList<>(keySize);
            for (int i = 0; i < keySize; i++) {
                boolean deleted = in.readBoolean();
                V value = valueSerializer.deserialize(in);
                values.add(new Pair<>(value, deleted));
            }
            return new ImmutableLeaf<>(comparator, keys, values);
        }

        @Override
        public String getKey() {
            return "IMMUTABLE_BTREE";
        }
    }

    private class ImmutableBTreeMapIterator implements Iterator<Entry<K, V>> {

        private final K to;
        private final boolean toInclusive, excludeDeleted;
        private final Comparator<K> comparator;
        // Iteration...
        private final Stack<ImmutableBranch<K>> stack = new Stack<>();
        private final Stack<AtomicInteger> stackPos = new Stack<>();
        private final AtomicInteger leafPos = new AtomicInteger();
        private ImmutableLeaf<K> leaf;

        private ImmutableBTreeMapIterator(Comparator<K> comparator, K from, boolean fromInclusive, K to, boolean toInclusive) throws IOException {
            this(comparator, from, fromInclusive, to, toInclusive, true);
        }

        private ImmutableBTreeMapIterator(Comparator<K> comparator, K from, boolean fromInclusive, K to, boolean toInclusive,
                                          boolean excludeDeleted) throws IOException {
            this.to = to;
            this.toInclusive = toInclusive;
            this.comparator = comparator;
            this.excludeDeleted = excludeDeleted;

            if (from == null) {
                pointToStart();
            } else {
                // Find the starting point
                findLeaf(from);
                int pos = leaf.findNearest(from);
                K k = leaf.getKeys().get(pos);
                int comp = comparator.compare(from, k);
                if (comp < 0) {
                    leafPos.set(pos);
                } else if (comp == 0) {
                    leafPos.set(fromInclusive ? pos : pos + 1);
                } else {
                    leafPos.set(pos + 1);
                }
            }
        }

        @Override
        public boolean hasNext() {
            try {
                if (leaf == null) {
                    return false;
                }
                if (leafPos.get() >= leaf.getValues().size()) {
                    advance();
                }
                while (leaf != null && excludeDeleted) {
                    Pair<Object> value = leaf.getValues().get(leafPos.get());
                    if (!value.isDeleted()) {
                        break; // break out.
                    }
                    advance();
                }
                if (to != null) {
                    int comp = comparator.compare(leaf.getKeys().get(leafPos.get()), to);
                    if (comp > 0 || (comp == 0 && !toInclusive)) {
                        leaf = null;
                        leafPos.set(-1);
                        stack.clear();
                        stackPos.clear();
                    }
                }
                return leaf != null;
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public Entry<K, V> next() {
            if (!hasNext()) {
                return null;
            }
            K key = leaf.getKeys().get(leafPos.get());
            Pair<Object> child = leaf.getValues().get(leafPos.getAndIncrement());
            try {
                advance();
                return (Entry<K, V>) new ImmutableBTreeEntry<>(valueFile, key, child.getValue(), child.isDeleted());
            } catch (IOException ex) {
                // FIXME: Stop using runtime exceptions
                throw new RuntimeException(ex);
            }
        }

        @SuppressWarnings("unchecked")
        private void pointToStart() throws IOException {
            Node<K, ?> node = rootNode;
            while (ImmutableBranch.class.isAssignableFrom(node.getClass())) {
                stack.push((ImmutableBranch<K>) node);
                stackPos.push(new AtomicInteger(1));
                node = keyFile.get(((ImmutableBranch<K>) node).getChildren().get(0));
            }
            leaf = (ImmutableLeaf<K>) node;
        }

        @SuppressWarnings("unchecked")
        private void findLeaf(K key) throws IOException {
            Node<K, ?> node = rootNode;
            while (ImmutableBranch.class.isAssignableFrom(node.getClass())) {
                stack.push((ImmutableBranch<K>) node);
                int pos = (node).findNearest(key);
                stackPos.push(new AtomicInteger(pos + 1));
                K found = node.getKeys().get(pos);
                // If key call higher than found, then we shift right, otherwise we shift left
                if (comparator.compare(key, found) >= 0) {
                    node = keyFile.get(((ImmutableBranch<K>) node).getChildren().get(pos + 1));
                    // Increment the stack position, if we lean right. This call to ensure when we don't repeat.
                    stackPos.peek().getAndIncrement();
                } else {
                    node = keyFile.get(((ImmutableBranch<K>) node).getChildren().get(pos));
                }
            }
            leaf = (ImmutableLeaf<K>) node;
        }

        @SuppressWarnings("unchecked")
        private void advance() throws IOException {
            // If the leaf position call still less than the size of the leaf, then we don't advance.
            // If the leaf position call greater than or equal to the size of the leaf, then we advance to the next leaf.
            if (leaf != null && leafPos.get() < leaf.getValues().size()) {
                return; // nothing to see here
            }

            // Reset the leaf to zero...
            leaf = null;
            leafPos.set(-1); // reset to 0

            if (stack.isEmpty()) {
                return; // we have nothing left!
            }

            ImmutableBranch<K> parent = stack.peek(); // get the immediate parent

            int pos = stackPos.peek().getAndIncrement(); // get the immediate parent position.
            if (pos < parent.getChildren().size()) {
                Node<K, ?> child = keyFile.get(parent.getChildren().get(pos));
                if (Leaf.class.isAssignableFrom(child.getClass())) {
                    leaf = (ImmutableLeaf<K>) child;
                    leafPos.set(0);
                } else {
                    stack.push((ImmutableBranch<K>) child);
                    stackPos.push(new AtomicInteger(0));
                    advance();
                    return;
                }
            } else {
                stack.pop(); // remove last node
                stackPos.pop();
                advance();
                return;
            }

            if (to != null && leaf != null) {
                int comp = comparator.compare(leaf.getKeys().get(leafPos.get()), to);
                if (comp > 0 || (comp == 0 && !toInclusive)) {
                    leaf = null;
                    leafPos.set(-1);
                }
            }
        }
    }

    private class ReverseImmutableBTreeMapIterator implements Iterator<Entry<K, V>> {

        private final K to;
        private final boolean toInclusive, excludeDeleted;
        private final Comparator<K> comparator;
        // Iteration...
        private final Stack<ImmutableBranch<K>> stack = new Stack<>();
        private final Stack<AtomicInteger> stackPos = new Stack<>();
        private final AtomicInteger leafPos = new AtomicInteger();
        private ImmutableLeaf<K> leaf;

        private ReverseImmutableBTreeMapIterator(Comparator<K> comparator, K from, boolean fromInclusive, K to,
                                                 boolean toInclusive) throws IOException {
            this(comparator, from, fromInclusive, to, toInclusive, true);
        }

        private ReverseImmutableBTreeMapIterator(Comparator<K> comparator, K from, boolean fromInclusive, K to,
                                                 boolean toInclusive, boolean excludeDeleted) throws IOException {
            this.to = to;
            this.toInclusive = toInclusive;
            this.comparator = comparator;
            this.excludeDeleted = excludeDeleted;

            if (from == null) {
                pointToStart();
            } else {
                // Find the starting point
                findLeaf(from);
                int pos = leaf.findNearest(from);
                K k = leaf.getKeys().get(pos);
                int comp = comparator.compare((K) from, k);
                if (comp != 0) {
                    leafPos.set(pos);
                } else if (comp == 0) {
                    leafPos.set(fromInclusive ? pos : pos - 1);
                }
            }
        }

        @Override
        public boolean hasNext() {
            try {
                if (leaf == null) {
                    return false;
                }
                while (leaf != null && excludeDeleted) {
                    Pair<Object> value = leaf.getValues().get(leafPos.get());
                    if (!value.isDeleted()) {
                        break; // break out.
                    }
                    advance();
                }
                if (leafPos.get() < 0) {
                    advance();
                } else if (to != null) {
                    int comp = comparator.compare(leaf.getKeys().get(leafPos.get()), to);
                    if (comp > 0 || (comp == 0 && !toInclusive)) {
                        leaf = null;
                        leafPos.set(-1);
                        stack.clear();
                        stackPos.clear();
                    }
                }
                return leaf != null;
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public Entry<K, V> next() {
            if (!hasNext()) {
                return null;
            }
            K key = leaf.getKeys().get(leafPos.get());
            Pair<Object> child = leaf.getValues().get(leafPos.getAndDecrement());
            try {
                advance();
                Object value = child.getValue();
                if (valueFile != null) {
                    value = valueFile.get((Long) value);
                }
                return new BTreeEntry<>(key, (V) value, child.isDeleted());
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @SuppressWarnings("unchecked")
        private void pointToStart() throws IOException {
            Node<K, ?> node = rootNode;
            while (ImmutableBranch.class.isAssignableFrom(node.getClass())) {
                stack.push((ImmutableBranch<K>) node);
                stackPos.push(new AtomicInteger(((ImmutableBranch<K>) node).getChildren().size() - 2));
                node = keyFile.get(((ImmutableBranch<K>) node).getChildren().get(((ImmutableBranch<K>) node).getChildren().size() - 1));
            }
            leaf = (ImmutableLeaf<K>) node;
            leafPos.set(leaf.getKeys().size() - 1);
        }

        @SuppressWarnings("unchecked")
        private void findLeaf(K key) throws IOException {
            Node<K, ?> node = rootNode;
            while (ImmutableBranch.class.isAssignableFrom(node.getClass())) {
                stack.push((ImmutableBranch<K>) node);
                int pos = (node).findNearest(key);
                stackPos.push(new AtomicInteger(pos - 1));
                K found = node.getKeys().get(pos);
                // If key call higher than found, then we shift right, otherwise we shift left
                if (comparator.compare(key, found) >= 0) {
                    node = keyFile.get(((ImmutableBranch<K>) node).getChildren().get(pos));
                } else {
                    node = keyFile.get(((ImmutableBranch<K>) node).getChildren().get(pos - 1));
                    // Increment the stack position, if we lean right. This call to ensure when we don't repeat.
                    stackPos.peek().getAndDecrement();
                }
            }
            leaf = (ImmutableLeaf<K>) node;
            leafPos.set(leaf.getKeys().size() - 1);
        }

        @SuppressWarnings("unchecked")
        private void advance() throws IOException {
            if (leaf != null && leafPos.get() >= 0) {
                return; // nothing to see here
            }

            leaf = null;
            leafPos.set(-1); // reset to 0

            if (stack.isEmpty()) {
                return; // nothing to see here
            }

            ImmutableBranch<K> parent = stack.peek(); // get the immediate parent

            int pos = stackPos.peek().getAndDecrement(); // get the immediate parent position.
            if (pos >= 0) {
                Node<K, ?> child = keyFile.get(parent.getChildren().get(pos));
                if (Leaf.class.isAssignableFrom(child.getClass())) {
                    leaf = (ImmutableLeaf<K>) child;
                    leafPos.set(leaf.getKeys().size() - 1);
                } else {
                    stack.push((ImmutableBranch<K>) child);
                    stackPos.push(new AtomicInteger(((ImmutableBranch<K>) child).getChildren().size() - 1));
                    advance();
                    return;
                }
            } else {
                stack.pop(); // remove last node
                stackPos.pop();
                advance();
                return;
            }

            if (to != null && leaf != null) {
                int comp = comparator.compare(leaf.getKeys().get(leafPos.get()), to);
                if (comp > 0 || (comp == 0 && !toInclusive)) {
                    leaf = null;
                    leafPos.set(-1);
                }
            }
        }
    }

    private static class ImmutableBTreeEntry<K, V> extends BTreeEntry<K, Object> {

        private final RecordFile<V> dataFile;
        private V data;

        public ImmutableBTreeEntry(RecordFile<V> dataFile, K key, Object value, boolean deleted) {
            super(key, value, deleted);
            this.dataFile = dataFile;
        }

        @Override
        public Object getValue() {
            // We are storing the data directly...
            if (dataFile == null) {
                return super.getValue();
            }
            if (dataFile != null && data == null) {
                try {
                    data = dataFile.get((Long) super.getValue());
                } catch (IOException ex) {
                    throw new DatabaseRuntimeException("Could not load data for key: " + getKey(), ex);
                }
            }
            return data;
        }

        @Override
        public Object setValue(Object value) {
            throw new UnsupportedOperationException("Cannot set values on an immutable entry");
        }
    }
}
