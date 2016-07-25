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
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.pungwe.db.core.error.DatabaseRuntimeException;
import com.pungwe.db.core.io.serializers.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BTreeMap<K, V> extends AbstractBTreeMap<K, V> {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Node<K, ?> root;
    private final AtomicLong size = new AtomicLong();
    private final BloomFilter<K> bloomFilter;
    private final Serializer<K> bloomSerializer;

    @SuppressWarnings("unchecked")
    public BTreeMap(Serializer<K> bloomSerializer, Comparator<K> comparator, int maxKeysPerNode) {
        super(comparator, maxKeysPerNode);
        root = new BTreeLeaf<>(comparator);
        this.bloomSerializer = bloomSerializer;
        bloomFilter = BloomFilter.create((from, into) -> {
            try {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes);
                BTreeMap.this.bloomSerializer.serialize(out, from);
                into.putBytes(bytes.toByteArray());
            } catch (IOException ex) {
                throw new DatabaseRuntimeException(ex);
            }
        }, maxKeysPerNode, 0.01);
    }

    @Override
    protected BloomFilter<K> bloomFilter() {
        return this.bloomFilter;
    }

    public static <K,V> BTreeMap<K, V> from(Serializer<K> keySerializer, AbstractBTreeMap<K, V> map,
                                            int maxKeysPerNode) {
        @SuppressWarnings("unchecked")
        BTreeMap<K,V> newMap = new BTreeMap<>(keySerializer, (Comparator<K>)map.comparator(), maxKeysPerNode);
        map.entrySet().forEach(newMap::putEntry);
        return newMap;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected BTreeEntry<K, V> putEntry(Entry<? extends K, ? extends V> entry) {
        if (entry == null || entry.getKey() == null) {
            return null;
        }
        lock.writeLock().lock();
        try {
            Stack<Node<K, ?>> stack = new Stack<>();
            Node<K, ?> node = root;
            stack.push(node);
            while (BTreeBranch.class.isAssignableFrom(node.getClass())) {
                int pos = node.findNearest(entry.getKey());
                K found = node.getKeys().get(pos);
                Node[] children = ((BTreeBranch<K>) node).get(found);
                if (comparator.compare(found, entry.getKey()) <= 0) {
                    // Swing to the right
                    node = children[1];
                } else {
                    node = children[0];
                }
                stack.push(node);
            }
            BTreeLeaf<K, V> leaf = (BTreeLeaf<K, V>) node;
            boolean newValue = true;
            if (leaf.findPosition(entry.getKey()) >= 0) {
                newValue = false;
            }
            leaf.put(entry.getKey(), new Pair<>(entry.getValue(), false));
            // Add to the bloom filter
            bloomFilter.put(entry.getKey());
            // Check and split
            checkAndSplit(stack);
            if (newValue) {
                size.getAndIncrement();
            }
            return new BTreeEntry<>(entry.getKey(), entry.getValue(), false);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @SuppressWarnings("unchecked")
    private void checkAndSplit(Stack<Node<K, ?>> parents) {
        if (parents.isEmpty()) {
            return; // Do nothing
        }
        Node<K, ?> node = parents.pop();
        if (node.getKeys().size() < maxKeysPerNode) {
            return; // do nothing
        }

        int mid = node.getKeys().size() - 1 >>> 1;
        // Key for split
        K key = node.getKeys().get(mid);

        // Split to left and right
        Node<K, ?>[] split = node.split();
        // New root node!!
        if (parents.isEmpty()) {
            root = new BTreeBranch<K>(comparator);
            // Ensure next and previous are set.
            ((BTreeBranch<K>) root).put(key, split);
            return;
        }
        // Pop the parent... This call to enable linking, then we will put it back!
        BTreeBranch<K> parent = (BTreeBranch<K>) parents.peek();
        parent.put(key, split);
        // Check and split again!
        checkAndSplit(parents);
    }

    @SuppressWarnings("unchecked")
    private BTreeLeaf<K, V> findLeafForGet(K key) {
        Node<K, ?> node = root;
        while (BTreeBranch.class.isAssignableFrom(node.getClass())) {
            int pos = (node).findNearest(key);
            K found = node.getKeys().get(pos);
            Node[] children = ((BTreeBranch<K>) node).get(found);
            int cmp = comparator.compare(found, key);
            if (cmp <= 0) {
                node = children[1];
            } else {
                node = children[0];
            }
        }
        return (BTreeLeaf<K, V>) node;
    }

    @Override
    public BTreeEntry<K, V> getEntry(K key) {
        lock.readLock().lock();
        try {
            BTreeLeaf<K, V> leaf = findLeafForGet(key);
            Pair<V> value = leaf == null ? null : leaf.get(key);
            return value == null || value.isDeleted() ? null : new BTreeEntry<>(key, value.getValue(),
                    value.isDeleted());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    protected V removeEntry(K key) {
        lock.writeLock().lock();
        try {
            BTreeLeaf<K, V> leaf = findLeafForGet(key);
            Pair<V> pair = leaf.get(key);
            if (pair == null) {
                return null;
            }
            pair.setDeleted(true);
            size.decrementAndGet();
            return pair.getValue();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    protected long sizeLong() {
        return size.get();
    }

    @Override
    protected Iterator<Entry<K, V>> iterator(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return new BTreeMapIterator(comparator, fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    protected Iterator<Entry<K, V>> reverseIterator(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return new ReverseBTreeMapIterator((o1, o2) -> -comparator.compare(o1, o2), fromKey, fromInclusive,
                toKey, toInclusive);
    }

    @Override
    protected Iterator<Entry<K, V>> mergeIterator() {
        return mergeIterator(null, false, null, false);
    }

    @Override
    protected Iterator<Entry<K, V>> mergeIterator(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return new BTreeMapIterator(comparator, fromKey, fromInclusive, toKey, toInclusive, false);
    }

    @Override
    protected Node<K, ?> rootNode() {
        return root;
    }

    @Override
    public void clear() {
        root = new BTreeLeaf<>(comparator);
    }

    // FIXME: This should be pushed up
    protected static class BTreeBranch<K> extends Branch<K, Node> {

        public BTreeBranch(Comparator<K> comparator) {
            super(comparator, new ArrayList<>());
        }

        public BTreeBranch(Comparator<K> comparator, List<K> keys, List<Node> children) {
            super(comparator, children);
            this.keys = new ArrayList<K>(keys.size());
            this.keys.addAll(keys);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void put(K key, Node[] value) {
            if (keys.isEmpty()) {
                keys.add(key);
                getChildren().addAll(Arrays.asList(value));
                return;
            }
            int nearest = findNearest(key);
            K found = keys.get(nearest);
            int cmp = comparator.compare(found, key);
            // Already exists, so replace it
            if (cmp < 0) {
                keys.add(nearest + 1, key);
                Node<K, ?> oldLeft = getChildren().get(nearest + 1);
                getChildren().set(nearest + 1, value[0]);
                getChildren().add(nearest + 2, value[1]);
                if (BTreeLeaf.class.isAssignableFrom(value[0].getClass())) {
                    ((BTreeLeaf)value[0]).setPrevious((BTreeLeaf)getChildren().get(nearest));
                    ((BTreeLeaf)getChildren().get(nearest)).setNext((BTreeLeaf)value[0]);
                    ((BTreeLeaf)value[1]).setNext(((BTreeLeaf)oldLeft).getNext());
                    if (((BTreeLeaf)oldLeft).getNext() != null) {
                        ((BTreeLeaf)oldLeft).getNext().setPrevious((BTreeLeaf) value[1]);
                    }
                }
            } else {
                keys.add(nearest, key);
                Node<K, ?> oldLeft = getChildren().get(nearest);
                getChildren().add(nearest, value[0]);
                getChildren().set(nearest + 1, value[1]);
                if (BTreeLeaf.class.isAssignableFrom(value[0].getClass())) {
                    ((BTreeLeaf)value[0]).setPrevious(((BTreeLeaf)oldLeft).getPrevious());
                    BTreeLeaf previous = ((BTreeLeaf) oldLeft).getPrevious();
                    if (previous != null) {
                        previous.setNext((BTreeLeaf)value[0]);
                    }
                    BTreeLeaf next = ((BTreeLeaf)oldLeft).getNext();
                    ((BTreeLeaf)value[1]).setNext(next);
                    if (next != null) {
                        next.setPrevious((BTreeLeaf)value[1]);
                    }
                }
            }
        }

        @Override
        public Node[] get(K key) {
            int pos = findNearest(key);
            if (pos > keys.size()) {
                pos -= 1;
            }
            return new Node[]{getChildren().get(pos), getChildren().get(pos + 1)};
        }

        @Override
        @SuppressWarnings("unchecked")
        public Node<K, Node[]>[] split() {
            int mid = keys.size() - 1 >>> 1;
            BTreeBranch<K> left = new BTreeBranch<>(comparator,
                    keys.subList(0, mid), getChildren().subList(0, mid + 1));
            BTreeBranch<K> right = new BTreeBranch<>(comparator,
                    keys.subList(mid + 1, keys.size()), getChildren().subList(mid + 1, getChildren().size()));
            return new Node[]{left, right};
        }
    }

    protected static class BTreeLeaf<K, V> extends Leaf<K, V, BTreeLeaf<K, V>> {

        private BTreeLeaf<K, V> previous, next;

        public BTreeLeaf(Comparator<K> comparator) {
            super(comparator);
        }

        public BTreeLeaf(Comparator<K> comparator, BTreeLeaf<K, V> previous, BTreeLeaf<K, V> next) {
            super(comparator);
            this.previous = previous;
            this.next = next;
        }

        public BTreeLeaf(Comparator<K> comparator, List<K> keys, List<Pair<V>> values) {
            super(comparator, keys, values);
        }

        public BTreeLeaf(Comparator<K> comparator, List<K> keys, List<Pair<V>> values,
                         BTreeLeaf<K, V> previous, BTreeLeaf<K, V> next) {
            super(comparator, keys, values);
            this.previous = previous;
            this.next = next;
        }

        public BTreeLeaf<K, V> getPrevious() {
            return previous;
        }

        public void setPrevious(BTreeLeaf<K, V> previous) {
            this.previous = previous;
        }

        public BTreeLeaf<K, V> getNext() {
            return next;
        }

        public void setNext(BTreeLeaf<K, V> next) {
            this.next = next;
        }

        // TODO: If this works, then promote
        @Override
        @SuppressWarnings("unchecked")
        public Node<K, Pair<V>>[] split() {
            int mid = keys.size() - 1 >>> 1;
            BTreeLeaf<K, V> left = newLeaf(keys.subList(0, mid), values.subList(0, mid));
            BTreeLeaf<K, V> right = newLeaf(keys.subList(mid, keys.size()), values.subList(mid, values.size()));
            left.setNext(right);
            right.setPrevious(left);
            return new Node[]{left, right};
        }

        @Override
        protected BTreeLeaf<K, V> newLeaf() {
            return new BTreeLeaf<>(comparator);
        }

        @Override
        protected BTreeLeaf<K, V> newLeaf(List<K> keys, List<Pair<V>> values) {
            return new BTreeLeaf<>(comparator, keys, values);
        }
    }

    private class BTreeMapIterator implements Iterator<Entry<K, V>> {

        private final K to;
        private final boolean toInclusive, excludeDeleted;
        private final Comparator<K> comparator;
        // Iteration...
        private final AtomicInteger leafPos = new AtomicInteger();
        private final AtomicLong counter = new AtomicLong();
        private BTreeLeaf<K, V> leaf;

        private BTreeMapIterator(Comparator<K> comparator, K from, boolean fromInclusive, K to, boolean toInclusive) {
            this(comparator, from, fromInclusive, to, toInclusive, true);
        }

        private BTreeMapIterator(Comparator<K> comparator, K from, boolean fromInclusive, K to, boolean toInclusive,
                                 boolean excludeDeleted) {
            this.to = to;
            this.toInclusive = toInclusive;
            this.comparator = comparator;
            this.excludeDeleted = excludeDeleted;

            lock.readLock().lock();
            try {
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
            } finally {
                lock.readLock().unlock();
            }
        }

        @Override
        public boolean hasNext() {
            lock.readLock().lock();
            try {
                if (leaf == null) {
                    return false;
                }
                if (leafPos.get() >= leaf.getValues().size()) {
                    advance();
                }
                while (leaf != null && excludeDeleted) {
                    Pair<V> value = leaf.getValues().get(leafPos.get());
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
                    }
                }
                return leaf != null;
            } finally {
                lock.readLock().unlock();
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public Entry<K, V> next() {
            lock.readLock().lock();
            try {
                if (!hasNext()) {
                    return null;
                }
                K key = leaf.getKeys().get(leafPos.get());
                Pair<V> child = leaf.getValues().get(leafPos.getAndIncrement());
                if (child != null) {
                    counter.incrementAndGet();
                }
                advance();
                return new BTreeEntry<>(key, child.getValue(), child.isDeleted());
            } finally {
                lock.readLock().unlock();
            }
        }

        @SuppressWarnings("unchecked")
        private void pointToStart() {
            Node<K, ?> node = root;
            while (BTreeBranch.class.isAssignableFrom(node.getClass())) {
                node = ((BTreeBranch<K>) node).getChildren().get(0);
            }
            leaf = (BTreeLeaf<K, V>) node;
        }

        @SuppressWarnings("unchecked")
        private void findLeaf(K key) {
            Node<K, ?> node = root;
            while (BTreeBranch.class.isAssignableFrom(node.getClass())) {
                int pos = (node).findNearest(key);
                K found = node.getKeys().get(pos);
                // If key call higher than found, then we shift right, otherwise we shift left
                if (comparator.compare(key, found) >= 0) {
                    node = ((BTreeBranch<K>) node).getChildren().get(pos + 1);
                } else {
                    node = ((BTreeBranch<K>) node).getChildren().get(pos);
                }
            }
            leaf = (BTreeLeaf<K, V>) node;
        }

        @SuppressWarnings("unchecked")
        private void advance() {
            // If the leaf position call still less than the size of the leaf, then we don't advance.
            // If the leaf position call greater than or equal to the size of the leaf, then we advance to the next leaf.
            if (leaf != null && leafPos.get() < leaf.getValues().size()) {
                return; // nothing to see here
            }

            // Move to the next leaf...
            BTreeLeaf<K, V> oldLeaf = leaf;
            leaf = leaf.getNext();
            leafPos.set(leaf != null ? 0 : -1); // reset to 0

            if (to != null && leaf != null) {
                int comp = comparator.compare(leaf.getKeys().get(leafPos.get()), to);
                if (comp > 0 || (comp == 0 && !toInclusive)) {
                    leaf = null;
                    leafPos.set(-1);
                }
            }
        }
    }

    private class ReverseBTreeMapIterator implements Iterator<Entry<K, V>> {

        private final K to;
        private final boolean toInclusive, excludeDeleted;
        private final Comparator<K> comparator;
        private final AtomicInteger leafPos = new AtomicInteger();
        private BTreeLeaf<K, V> leaf;

        private ReverseBTreeMapIterator(Comparator<K> comparator, K from, boolean fromInclusive, K to,
                                        boolean toInclusive) {
            this(comparator, from, fromInclusive, to, toInclusive, true);
        }

        private ReverseBTreeMapIterator(Comparator<K> comparator, K from, boolean fromInclusive, K to,
                                        boolean toInclusive, boolean excludeDeleted) {
            this.to = to;
            this.toInclusive = toInclusive;
            this.comparator = comparator;
            this.excludeDeleted = excludeDeleted;

            lock.readLock().lock();
            try {
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
            } finally {
                lock.readLock().unlock();
            }
        }

        @Override
        public boolean hasNext() {
            lock.readLock().lock();
            try {
                if (leaf == null) {
                    return false;
                }
                while (leaf != null && excludeDeleted) {
                    Pair<V> value = leaf.getValues().get(leafPos.get());
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
                    }
                }
                return leaf != null;
            } finally {
                lock.readLock().unlock();
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public Entry<K, V> next() {
            lock.readLock().lock();
            try {
                if (!hasNext()) {
                    return null;
                }
                K key = leaf.getKeys().get(leafPos.get());
                Pair<V> child = leaf.getValues().get(leafPos.getAndDecrement());
                advance();
                return new BTreeEntry<>(key, child.getValue(), child.isDeleted());
            } finally {
                lock.readLock().unlock();
            }
        }

        @SuppressWarnings("unchecked")
        private void pointToStart() {
            Node<K, ?> node = root;
            while (BTreeBranch.class.isAssignableFrom(node.getClass())) {
                node = ((BTreeBranch<K>) node).getChildren().get(((BTreeBranch<K>) node).getChildren().size() - 1);
            }
            leaf = (BTreeLeaf<K, V>) node;
            leafPos.set(leaf.getKeys().size() - 1);
        }

        @SuppressWarnings("unchecked")
        private void findLeaf(K key) {
            Node<K, ?> node = root;
            while (BTreeBranch.class.isAssignableFrom(node.getClass())) {
                int pos = (node).findNearest(key);
                K found = node.getKeys().get(pos);
                // If key call higher than found, then we shift right, otherwise we shift left
                if (comparator.compare(key, found) >= 0) {
                    node = ((BTreeBranch<K>) node).getChildren().get(pos);
                } else {
                    node = ((BTreeBranch<K>) node).getChildren().get(pos - 1);
                }
            }
            leaf = (BTreeLeaf<K, V>) node;
            leafPos.set(leaf.getKeys().size() - 1);
        }

        @SuppressWarnings("unchecked")
        private void advance() {
            if (leaf != null && leafPos.get() >= 0) {
                return; // nothing to see here
            }

            leaf = leaf.getPrevious();
            leafPos.set(leaf != null ? leaf.getKeys().size() - 1 : -1);

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
