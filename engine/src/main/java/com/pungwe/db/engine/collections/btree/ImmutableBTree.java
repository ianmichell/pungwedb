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

/**
 * Created by ian on 09/07/2016.
 */
public class ImmutableBTree<K,V> extends AbstractBTreeMap<K,V> {

    private final RecordFile<Node<K, ?>> recordFile;
    private long size;
    private final Node<K, ?> rootNode;

    private ImmutableBTree(Comparator<K> comparator, RecordFile<Node<K, ?>> recordFile) throws IOException {
        super(comparator);
        this.recordFile = recordFile;
        this.rootNode = findRootNode();
    }

    public static <K, V> Serializer<Node<K, ?>> serializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        return null;
    }

    public static <K,V> ImmutableBTree<K,V> write(RecordFile<Node<K, ?>> recordFile, String treeName,
                                                  AbstractBTreeMap<K,V> treeToWrite) throws IOException {
        BTreeWriter<K,V> writer = new BTreeWriter<>(recordFile);
        return writer.write(treeName, treeToWrite);
    }

    public static <K,V> ImmutableBTree<K,V> getInstance(Comparator<K> comparator, RecordFile<Node<K, ?>> recordFile)
            throws IOException {
        return new ImmutableBTree<>(comparator, recordFile);
    }

    private Node<K, ?> findRootNode() throws IOException {
        Map<String, Object> metaData = recordFile.getMetaData();
        Object size = metaData.getOrDefault("size", 0);
        Object rootPointer = metaData.get("root");
        long root = Number.class.isAssignableFrom(rootPointer.getClass()) ? ((Number)rootPointer).longValue() :
                new Long(rootPointer.toString());
        this.size = Number.class.isAssignableFrom(size.getClass()) ? ((Number)size).longValue() :
                new Long(size.toString());
        // Get the root node!
        return recordFile.get(root);
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
            Leaf<K, V> leaf = findLeafForKey(key);
            Pair<V> pair = leaf.get(key);
            if (pair == null || pair.isDeleted()) {
                return null;
            }
            return new BTreeEntry<>(key, pair.getValue(), pair.isDeleted());
        } catch (IOException ex) {
            // There was an issue reading...
            throw new RuntimeException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private Leaf findLeafForKey(K key) throws IOException {
        Node<K, ?> node = rootNode;
        while (Branch.class.isAssignableFrom(node.getClass())) {
            int pos = (node).findNearest(key);
            K found = node.getKeys().get(pos);
            Long[] children = ((ImmutableBranch<K>) node).get(found);
            int cmp = comparator.compare(found, key);
            if (cmp <= 0) {
                node = recordFile.get(children[1]);
            } else {
                node = recordFile.get(children[0]);
            }
        }
        return (Leaf<K,V>)node;
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
        return null;
    }

    @Override
    protected Iterator<Entry<K, V>> reverseIterator(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return null;
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
            return new Long[] { getChildren().get(pos), getChildren().get(pos + 1) };
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

    public static class BTreeWriter<K,V> implements Closeable {

        private final RecordFile<Node<K, ?>> recordFile;

        // FIXME: Record file might not be the way forward...
        private BTreeWriter(RecordFile<Node<K, ?>> recordFile) {
            this.recordFile = recordFile;
        }

        /**
         * Writes the given btree to a <code>RecordFile</code> and returns an instance of an <code>ImmutableBTree</code>
         *
         * @param btreeToWrite an instance of <code>AbstractBTree</code> to write.
         *
         * @return an instance of <code>ImmutableBTree</code>
         *
         * @throws IOException if there is a problem writing to the <code>RecordFile</code>
         */
        @SuppressWarnings("unchecked")
        public ImmutableBTree<K,V> write(String name, AbstractBTreeMap<K,V> btreeToWrite) throws IOException {
            RecordFile.Writer<Node<K, ?>> writer = recordFile.writer();
            Node<K, ?> root = btreeToWrite.rootNode();
            long rootPosition = 0;
            if (Leaf.class.isAssignableFrom(root.getClass())) {
                // Root is a leaf and we should simply just write it to disk...
                rootPosition = writer.append(root);
            } else {
                // Loop through each key in the root node...
                List<Long> children = writeChildren(writer, (Branch<K, Node>) root);
                ImmutableBranch<K> newRoot = new ImmutableBranch<K>(root.comparator, root.getKeys(), children);
                rootPosition = writer.append(newRoot);
            }
            // Write the meta data
            Map<String, Object> metaData = new LinkedHashMap<>();
            metaData.put("root", rootPosition);
            metaData.put("name", name);
            metaData.put("size", btreeToWrite.sizeLong());
            recordFile.setMetaData(metaData);
            // Sync the record to disk
            writer.sync();
            // Once the root node is written, then return a new instance of the ImmutableBTree
            return new ImmutableBTree<>((Comparator<K>)btreeToWrite.comparator(), recordFile);
        }

        @SuppressWarnings("unchecked")
        private List<Long> writeChildren(RecordFile.Writer<Node<K, ?>> writer, Branch<K, Node> branch)
                throws IOException {
            // Use a linked list because they are nice and quick
            List<Long> children = new LinkedList<>();
            // Loop through each child and serialize.
            for (Node<K, ?> child : branch.getChildren()) {
                // If child is a leaf, we can simply write it.
                if (Leaf.class.isAssignableFrom(child.getClass())) {
                    long position = writer.append(child);
                    children.add(position);
                    continue;
                }
                // If the child is a branch, we need to do exactly the same thing
                List<Long> grandChildren = writeChildren(writer, (Branch<K, Node>)child);
                ImmutableBranch<K> immutableChild = new ImmutableBranch<K>(child.comparator, child.getKeys(),
                        grandChildren);
                // write the immutableChild to the record file
                long position = writer.append(immutableChild);
                children.add(position);
            }
            return children;
        }

        @Override
        public void close() throws IOException {

        }
    }

    private static class ImmutableNodeSerializer<K, V> implements Serializer<Node<K, ?>> {

        private final Serializer<K> keySerializer;
        private final Serializer<V> valueSerializer;

        public ImmutableNodeSerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        @Override
        public void serialize(DataOutput out, Node<K, ?> value) throws IOException {

        }

        @Override
        public Node<K, ?> deserialize(DataInput in) throws IOException {
            return null;
        }

        @Override
        public String getKey() {
            return "IMMUTABLE_BTREE";
        }
    }
}
