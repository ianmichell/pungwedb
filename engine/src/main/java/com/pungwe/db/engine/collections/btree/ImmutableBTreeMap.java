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

import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.engine.io.RecordFile;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ian on 09/07/2016.
 */
public class ImmutableBTreeMap<K, V> extends AbstractBTreeMap<K, V> {

    private final RecordFile<Node<K, ?>> keyFile;
    private final RecordFile<V> valueFile;
    private long size;
    private final Node<K, ?> rootNode;

    private ImmutableBTreeMap(Comparator<K> comparator, RecordFile<Node<K, ?>> keyFile,
                              RecordFile<V> valueFile) throws IOException {
        super(comparator);
        this.keyFile = keyFile;
        this.valueFile = valueFile;
        this.rootNode = findRootNode();
    }

    public static <K, V> Serializer<Node<K, ?>> serializer(Comparator<K> comparator, Serializer<K> keySerializer,
                                                           Serializer<V> valueSerializer) {
        return new ImmutableNodeSerializer<>(comparator, keySerializer, valueSerializer);
    }

    public static <K, V> ImmutableBTreeMap<K, V> write(RecordFile<Node<K, ?>> recordFile, String treeName,
                                                       AbstractBTreeMap<K, V> treeToWrite) throws IOException {
        // Write data inline...
        return write(recordFile, null, treeName, treeToWrite);
    }

    public static <K, V> ImmutableBTreeMap<K, V> write(RecordFile<Node<K, ?>> keyFile, RecordFile<V> valueFile,
                                                       String treeName, AbstractBTreeMap<K, V> treeToWrite)
            throws IOException {
        BTreeWriter<K, V> writer = new BTreeWriter<>(keyFile, valueFile);
        return writer.write(treeName, treeToWrite);
    }

    public static <K, V> ImmutableBTreeMap<K, V> merge(RecordFile<Node<K, ?>> keyFile, RecordFile<V> valueFile,
                                                       String treeName, int maxKeysPerNode, AbstractBTreeMap<K, V> left,
                                                       AbstractBTreeMap<K, V> right, boolean gc) throws IOException {

        BTreeMergeWriter<K, V> writer = new BTreeMergeWriter<>(maxKeysPerNode, keyFile, valueFile);
        return writer.merge(treeName, left, right, gc);
    }

    public static <K, V> ImmutableBTreeMap<K, V> getInstance(Comparator<K> comparator,
                                                             RecordFile<Node<K, ?>> recordFile, RecordFile<V> valueFile)
            throws IOException {
        return new ImmutableBTreeMap<>(comparator, recordFile, valueFile);
    }

    private Node<K, ?> findRootNode() throws IOException {
        Map<String, Object> metaData = keyFile.getMetaData();
        Object size = metaData.getOrDefault("size", 0);
        Object rootPointer = metaData.get("root");
        long root = Number.class.isAssignableFrom(rootPointer.getClass()) ? ((Number) rootPointer).longValue() :
                new Long(rootPointer.toString());
        this.size = Number.class.isAssignableFrom(size.getClass()) ? ((Number) size).longValue() :
                new Long(size.toString());
        // Get the root node!
        return keyFile.get(root);
    }

    @Override
    protected Node<K, ?> rootNode() {
        return rootNode;
    }

    @Override
    protected BTreeEntry<K, V> putEntry(Entry<? extends K, ? extends V> entry) {
        throw new UnsupportedOperationException("This BTree is immutable");
    }

    @Override
    @SuppressWarnings("unchecked")
    protected BTreeEntry<K, V> getEntry(K key) {
        try {
            ImmutableLeaf<K> leaf = findLeafForKey(key);
            Pair<Object> pair = leaf.get(key);
            if (pair == null || pair.isDeleted()) {
                return null;
            }

            Object value = pair.getValue();
            if (valueFile != null) {
                // If the value file is not null, then we are going to expect a long.
                value = valueFile.get((Long) value);
            }

            return new BTreeEntry<>(key, (V) value, pair.isDeleted());
        } catch (IOException ex) {
            // There was an issue reading...
            throw new RuntimeException(ex);
        }
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
        throw new UnsupportedOperationException("This BTree is immutable");
    }

    @Override
    protected long sizeLong() {
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
    protected Iterator<Entry<K, V>> mergeIterator() {
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

    public static class ImmutableBranch<K> extends Branch<K, Long> {

        public ImmutableBranch(Comparator<K> comparator, List<K> keys, List<Long> children) {
            super(comparator, Collections.unmodifiableList(children));
            assert children.size() - keys.size() == 1;
            this.keys = Collections.unmodifiableList(keys);
        }

        @Override
        public void put(K key, Long[] value) {
            throw new UnsupportedOperationException("Cannot put on a read-only tree");
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

        // FIXME: Record file might not be the way forward...
        private BTreeWriter(RecordFile<Node<K, ?>> keyFile, RecordFile<V> valueFile) {
            this.keyFile = keyFile;
            this.valueFile = valueFile;
        }

        /**
         * Writes the given btree to a <code>RecordFile</code> and returns an instance of an <code>ImmutableBTreeMap</code>
         *
         * @param btreeToWrite an instance of <code>AbstractBTree</code> to write.
         * @return an instance of <code>ImmutableBTreeMap</code>
         * @throws IOException if there is a problem writing to the <code>RecordFile</code>
         */
        @SuppressWarnings("unchecked")
        public ImmutableBTreeMap<K, V> write(String name, AbstractBTreeMap<K, V> btreeToWrite) throws IOException {
            RecordFile.Writer<Node<K, ?>> keyWriter = keyFile.writer();
            RecordFile.Writer<V> valueWriter = valueFile != null ? valueFile.writer() : null;
            Node<K, ?> root = btreeToWrite.rootNode();
            long rootPosition = 0;
            if (Leaf.class.isAssignableFrom(root.getClass())) {
                // Root is a leaf and we should simply just write it to disk...
                rootPosition = keyWriter.append(root);
            } else {
                List<Long> children = writeChildren(keyWriter, valueWriter, (Branch<K, Node>) root);
                ImmutableBranch<K> newRoot = new ImmutableBranch<>(root.comparator, root.getKeys(), children);
                rootPosition = keyWriter.append(newRoot);
            }
            // Write the meta data
            Map<String, Object> metaData = new LinkedHashMap<>();
            metaData.put("root", rootPosition);
            metaData.put("name", name);
            metaData.put("size", btreeToWrite.sizeLong());
            keyFile.setMetaData(metaData);
            // Sync the record to disk
            keyWriter.commit();
            // Once the root node is written, then return a new instance of the ImmutableBTreeMap
            return new ImmutableBTreeMap<>((Comparator<K>) btreeToWrite.comparator(), keyFile, valueFile);
        }

        @SuppressWarnings("unchecked")
        private List<Long> writeChildren(RecordFile.Writer<Node<K, ?>> keyWriter,
                                         RecordFile.Writer<V> valueWriter, Branch<K, Node> branch)
                throws IOException {
            // Use a linked list because they are nice and quick
            List<Long> children = new LinkedList<>();
            // Loop through each child and serialize.
            for (Node<K, ?> child : branch.getChildren()) {
                // If child is a leaf, we can simply write it.
                List<K> keys = child.getKeys();
                if (Leaf.class.isAssignableFrom(child.getClass())) {
                    List<Pair<Object>> values = new LinkedList<>();
                    if (valueFile != null) {
                        for (Pair<Object> value : values) {
                            long position = valueWriter.append((V) value.getValue());
                            values.add(new Pair<>(position, value.isDeleted()));
                        }
                    } else {
                        values.addAll(((Leaf) child).getValues());
                    }
                    ImmutableLeaf<K> leaf = new ImmutableLeaf<>(child.comparator, keys, values);
                    long position = keyWriter.append(leaf);
                    children.add(position);
                    continue;
                }
                // If the child is a branch, we need to do exactly the same thing
                List<Long> grandChildren = writeChildren(keyWriter, valueWriter, (Branch<K, Node>) child);
                // Add child to immutable branch.
                ImmutableBranch<K> immutableChild = new ImmutableBranch<>(child.comparator, child.getKeys(),
                        grandChildren);
                // write the immutableChild to the record file
                long position = keyWriter.append(immutableChild);
                children.add(position);
            }
            return children;
        }

        @Override
        public void close() throws IOException {

        }
    }

    private static class BranchPair<K> {
        protected List<K> keys = new LinkedList<>();
        protected List<Long> children = new LinkedList<>();
    }

    public static class BTreeMergeWriter<K, V> {

        private final int maxKeysPerNode;
        private final RecordFile<Node<K, ?>> keyFile;
        private final RecordFile<V> valueFile;
        private final RecordFile.Writer<Node<K, ?>> writer;

        /**
         * Construct a new instance of the merge writer with the given node size and a record file.
         * <p>
         * N.B. This will be much quicker if the maxKeysPerNode is higher than that of the trees passing in, however
         * some rationalisation is required and sensible number of keys per node on the bigger trees (like 1000) is
         * the most sensible, otherwise it could be slower.
         *
         * @param maxKeysPerNode the maximum number of keys per node
         * @param keyFile        the file for the tree to be written
         * @param valueFile      the file where the values for each key is written (null if inline is required).
         * @throws IOException if there is a problem writing the tree.
         */
        public BTreeMergeWriter(int maxKeysPerNode, RecordFile<Node<K, ?>> keyFile, RecordFile<V> valueFile)
                throws IOException {
            this.maxKeysPerNode = maxKeysPerNode;
            this.keyFile = keyFile;
            this.valueFile = valueFile;
            this.writer = keyFile.writer();
        }

        /**
         * Merging multiple trees is done by iterating jointly through each of the specified tress. This
         * method is limited to two trees, which can then be chained in a parallel process to create an even bigger
         * merge.
         * <p>
         * The left tree will yield to the right as it's assumed newer. If the current left and right keys are the same
         * then the right will be considered newer and replace the left.
         * <p>
         * The method will simply walk through each entry in both trees and combine them in order.
         * <p>
         * Please note that the comparator from the left is used, but asserts that the trees comparators are the same.
         * This is done by comparing the first key from each tree with both comparators from left to right and ensuring
         * that both comparators return the same result. If the keys are identical, we will increment the left key and
         * check them again, until we find a comparison greater than or less than 0. If the comparators return different
         * results, then an IOException will be thrown.
         * <p>
         * N.B. Trees themselves to do not have to be of the same type. For example I can merge a <code>BTreeMap</code>
         * with an <code>ImmutableBtreeMap</code>.
         *
         * @param name  the name of the new tree
         * @param left  tree to the left
         * @param right tree to the right
         * @param gc    Garbage Collection. When true this option will not merge deletions.
         * @return a new immutable instance of the combined b+tree.
         * @throws IOException
         */
        @SuppressWarnings("unchecked")
        public ImmutableBTreeMap<K, V> merge(String name, AbstractBTreeMap<K, V> left, AbstractBTreeMap<K, V> right,
                                             boolean gc) throws IOException {

            if (!isSameDirection(left, right)) {
                throw new IOException("Trees are not ordered in the same direction...");
            }

            // We want to iterate the whole thing form start to finish!
            final Iterator<Entry<K, V>> leftIT = left.mergeIterator();
            final Iterator<Entry<K, V>> rightIT = right.mergeIterator();

            // One of these iterators will have next!
            boolean hasNext = leftIT.hasNext() || rightIT.hasNext();

            AtomicLong treeSize = new AtomicLong();

            // Get the left and right entries to populate the next leaf node...
            BTreeEntry<K, V>[] leftRight = new BTreeEntry[2];

            // Stack or a Queue, that's the ultimate question
            Stack<BranchPair<K>> parents = new Stack<>();
            // Push a branch pair into parents to record the position of the immutable branch.
            parents.push(new BranchPair<K>());
            // We need a double loop to ensure that we can create branches...
            while (hasNext) {

                // Create a leaf to add to the branch... Doesn't return anything as it populates keys and children
                buildLeaf(treeSize, left.comparator, parents.peek().keys, parents.peek().children,
                        leftRight, leftIT, rightIT, gc);

                // Has next will check if we have any entries left to process.
                hasNext = leftIT.hasNext() || rightIT.hasNext() || leftRight[0] != null || leftRight[1] != null;

                // If there are no keys then! We have a root leaf...
                if (!hasNext && parents.peek().keys.size() < 1) {
                    // Write the meta data
                    Map<String, Object> metaData = new LinkedHashMap<>();
                    metaData.put("root", parents.peek().children.get(0));
                    metaData.put("name", name);
                    metaData.put("size", treeSize.get());
                    keyFile.setMetaData(metaData);
                    // Sync the record to disk
                    writer.commit();
                    // Write the meta data
                    return new ImmutableBTreeMap<>(left.comparator, keyFile, valueFile);
                }

                if (hasNext && parents.peek().keys.size() < maxKeysPerNode) {
                    continue;
                }

                // If there are keys then we need to create a new branch...
                final ImmutableBranch<K> branch = new ImmutableBranch<>(left.comparator, parents.peek().keys,
                        parents.peek().children);

                // Pop this branch out, so that we can write to it's parent.
                parents.pop();

                // Add a new pair if there are no parents...
                if (parents.size() == 0) {
                    parents.push(new BranchPair<>());
                }

                // Write the new branch to the record file.
                long position = writer.append(branch);

                // If there are other children. Then we add the first key to parent
                if (parents.peek().children.size() > 0) {
                    // We should never get an out of bounds exception here. If we do something is not working...
                    parents.peek().keys.add(branch.getKeys().get(0));
                }
                // Add the position to the parent's children.
                parents.peek().children.add(position);

                // If the new branch has less keys than maxKeysPerNode, then we're done and can't progress!
                if (branch.getKeys().size() < maxKeysPerNode) {
                    break; // Break out of this loop, we're at the end!
                }

                // If we get here then we have already hit the keys per branch limit and need to split
                buildBranches(parents, left.comparator);

                // At the end of everything we need to push a new BranchPair in, to start the next branch (if any).
                parents.push(new BranchPair<>());
            }

            // Write the remaining parents to disk. Hopefully in most cases we have only the root node left
            while (parents.size() > 0) {
                BranchPair<K> pair = parents.pop();
                // If the pair has no keys, this is a root node
                if (pair.keys.isEmpty()) {
                    // Write the meta data
                    Map<String, Object> metaData = new LinkedHashMap<>();
                    metaData.put("root", pair.children.get(0));
                    metaData.put("name", name);
                    metaData.put("size", treeSize.get());
                    keyFile.setMetaData(metaData);
                    // Sync the record to disk
                    writer.commit();
                    // Write the meta data
                    return new ImmutableBTreeMap<>(left.comparator, keyFile, valueFile);
                }

                // Create a new branch
                ImmutableBranch<K> branch = new ImmutableBranch<>(left.comparator, pair.keys, pair.children);
                // Write the branch
                long position = writer.append(branch);
                // If the key size of the branch is less than maxKeysPerNode, we're done
                if (branch.getKeys().size() < maxKeysPerNode || parents.size() == 0) {
                    // Write the meta data
                    Map<String, Object> metaData = new LinkedHashMap<>();
                    metaData.put("root", position);
                    metaData.put("name", name);
                    metaData.put("size", treeSize.get());
                    keyFile.setMetaData(metaData);
                    // Sync the record to disk
                    writer.commit();
                    // Write the meta data
                    return new ImmutableBTreeMap<>(left.comparator, keyFile, valueFile);
                }
                if (parents.peek().children.size() > 0) {
                    parents.peek().keys.add(branch.getKeys().get(0));
                }
                parents.peek().children.add(position);
            }

            throw new IOException("Could not appropriately merge trees");
        }

        /**
         * Create a parent branch and keep doing it until we get to the root.
         *
         * @param parents    the stack of parents to split into.
         * @param comparator the key comparator.
         * @throws IOException if there is an issue writing the tree.
         */
        private void buildBranches(final Stack<BranchPair<K>> parents, final Comparator<K> comparator)
                throws IOException {
            // We have to 1 or more parents.
            if (parents.size() > 0) {

                // If the keys are empty we shouldn't be processing this branch as it's the root.
                if (parents.peek().keys.isEmpty() && parents.size() == 1) {
                    return;
                }

                // Remove the last item from the stack... as we're going to use it for a new immutable
                BranchPair<K> pair = parents.pop();

                // Create a new immutable branch with the pair's keys and children. This is a parent branch
                ImmutableBranch<K> branch = new ImmutableBranch<>(comparator, pair.keys, pair.children);

                // Write branch to record file
                long position = writer.append(branch);

                // If parents is empty, then this is the root node.
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
                buildBranches(parents, comparator);
            }
        }

        private void buildLeaf(final AtomicLong treeSize, final Comparator<K> comparator,
                               final List<K> keys, final List<Long> children,
                               final BTreeEntry<K, V>[] leftRight, final Iterator<Entry<K, V>> leftIT,
                               final Iterator<Entry<K, V>> rightIT, boolean gc) throws IOException {

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
                 * If both are not null, we can compare them. If one is greater than the other then the greater is
                 * ignored and the lesser is added to the leaf.
                 */
                if (leftRight[0] != null && leftRight[1] != null) {
                    int cmp = comparator.compare(leftRight[0].getKey(), leftRight[1].getKey());
                    // If the left is the same as the right, then we add the right
                    if (cmp == 0) {

                        /*
                         * If we have a deleted value and gc is true, then we don't write anything!
                         */
                        if (!gc && !leftRight[1].isDeleted()) {
                            /*
                             * We want to avoid additional comparison, so need to add the key and value directly
                             * to the end of the list.
                             */
                            leaf.getKeys().add(leftRight[1].getKey());
                            leaf.getValues().add(new Pair<>(leftRight[1].getValue(), leftRight[1].isDeleted()));
                            // Ensure the right and left entries are set null
                            leftRight[0] = null;
                            leftRight[1] = null;

                            // Increment tree size
                            treeSize.incrementAndGet();
                        }
                    } else if (cmp < 0) {
                        // Left is the lesser entry, so add it to the leaf
                        leaf.getKeys().add(leftRight[0].getKey());
                        leaf.getValues().add(new Pair<>(leftRight[0].getValue(), leftRight[0].isDeleted()));
                        // Ensure that the left entry is set to null, so that it's not reprocessed
                        leftRight[0] = null;
                        // Increment tree size
                        treeSize.incrementAndGet();
                    } else {
                        // Left is greater than right
                        leaf.getKeys().add(leftRight[1].getKey());
                        leaf.getValues().add(new Pair<>(leftRight[1].getValue(), leftRight[1].isDeleted()));
                        // Ensure the right entry is set to null, so that it's not reprocessed
                        leftRight[1] = null;
                        // Increment tree size
                        treeSize.incrementAndGet();
                    }
                } else if (leftRight[0] == null && leftRight[1] != null) {
                    // If the left entry is null, then we simply add the right...
                    leaf.getKeys().add(leftRight[1].getKey());
                    leaf.getValues().add(new Pair<>(leftRight[1].getValue(), leftRight[1].isDeleted()));
                    // Ensure the right entry is set to null, so that it's not reprocessed
                    leftRight[1] = null;
                    // Increment tree size
                    treeSize.incrementAndGet();
                } else if (leftRight[1] == null && leftRight[0] != null) {
                    // If the right entry is null, then we simply add the left.
                    leaf.getKeys().add(leftRight[0].getKey());
                    leaf.getValues().add(new Pair<>(leftRight[0].getValue(), leftRight[0].isDeleted()));
                    // Ensure that the left entry is set to null, so that it's not reprocessed
                    leftRight[0] = null;
                    // Increment tree size
                    treeSize.incrementAndGet();
                }

                // If the leaf is now full, we need to write it and start a split.
                if (leaf.getKeys().size() == maxKeysPerNode) {

                    long position = writer.append(leaf);
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
            // Leaf is not full! But we will have to write it anyway as we're out of elements
            long position = writer.append(leaf);
            if (children.size() > 0) {
                keys.add(leaf.getKeys().get(0));
            }
            children.add(position);
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
            // Is this a branch?
            out.writeBoolean(Branch.class.isAssignableFrom(value.getClass()));
            // Write the number of keys...
            out.writeInt(value.getKeys().size());
            for (K key : value.getKeys()) {
                out.writeByte('E');
                keySerializer.serialize(out, key);
            }
            if (Branch.class.isAssignableFrom(value.getClass())) {
                out.writeInt(((Branch<K, Long>) value).getChildren().size());
                for (Long child : ((Branch<K, Long>) value).getChildren()) {
                    out.writeByte('E');
                    out.writeLong(child);
                }
            } else {
                out.writeInt(value.getKeys().size());
                for (Pair<Object> v : ((ImmutableLeaf<K>) value).getValues()) {
                    out.writeByte('E');
                    out.writeBoolean(v.isDeleted());
                    valueSerializer.serialize(out, (V) v.getValue());
                }
            }
        }

        @Override
        public Node<K, ?> deserialize(DataInput in) throws IOException {
            boolean branch = in.readBoolean();
            int keySize = in.readInt();
            List<K> keys = new ArrayList<>(keySize);
            for (int i = 0; i < keySize; i++) {
                assert in.readByte() == 'E'; // E for entry
                K key = keySerializer.deserialize(in);
                keys.add(key);
            }
            int valueSize = in.readInt();
            if (branch) {
                List<Long> children = new ArrayList<>(valueSize);
                for (int i = 0; i < valueSize; i++) {
                    assert in.readByte() == 'E';
                    Long child = in.readLong();
                    children.add(child);
                }
                return new ImmutableBranch<K>(comparator, keys, children);
            }
            List<Pair<Object>> values = new ArrayList<>(valueSize);
            for (int i = 0; i < valueSize; i++) {
                assert in.readByte() == 'E';
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
                int comp = comparator.compare((K) from, k);
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
                Object value = child.getValue();
                if (valueFile != null) {
                    value = valueFile.get((Long) value);
                }
                return new BTreeEntry<>(key, (V) value, child.isDeleted());
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
                // If key is higher than found, then we shift right, otherwise we shift left
                if (comparator.compare(key, found) >= 0) {
                    node = keyFile.get(((ImmutableBranch<K>) node).getChildren().get(pos + 1));
                    // Increment the stack position, if we lean right. This is to ensure that we don't repeat.
                    stackPos.peek().getAndIncrement();
                } else {
                    node = keyFile.get(((ImmutableBranch<K>) node).getChildren().get(pos));
                }
            }
            leaf = (ImmutableLeaf<K>) node;
        }

        @SuppressWarnings("unchecked")
        private void advance() throws IOException {
            // If the leaf position is still less than the size of the leaf, then we don't advance.
            // If the leaf position is greater than or equal to the size of the leaf, then we advance to the next leaf.
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
                // If key is higher than found, then we shift right, otherwise we shift left
                if (comparator.compare(key, found) >= 0) {
                    node = keyFile.get(((ImmutableBranch<K>) node).getChildren().get(pos));
                } else {
                    node = keyFile.get(((ImmutableBranch<K>) node).getChildren().get(pos - 1));
                    // Increment the stack position, if we lean right. This is to ensure that we don't repeat.
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
}
