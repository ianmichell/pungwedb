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

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ian on 15/07/2016.
 */
public class BTreeMap<K, V> extends AbstractBTreeMap<K, V> {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final int maxKeysPerNode;
    private Node<K, ?> root;
    private final AtomicLong size = new AtomicLong();

    public BTreeMap(Comparator<K> comparator, int maxKeysPerNode) {
        super(comparator);
        this.maxKeysPerNode = maxKeysPerNode;
        root = new BPlusTreeLeaf<>(comparator);
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
            while (BPlusTreeBranch.class.isAssignableFrom(node.getClass())) {
                int pos = node.findNearest(entry.getKey());
                K found = node.getKeys().get(pos);
                Node[] children = ((BPlusTreeBranch<K>) node).get(found);
                if (comparator.compare(found, entry.getKey()) <= 0) {
                    // Swing to the right
                    node = children[1];
                } else {
                    node = children[0];
                }
                stack.push(node);
            }
            BPlusTreeLeaf<K, V> leaf = (BPlusTreeLeaf<K, V>) node;
            boolean newValue = true;
            if (leaf.findPosition(entry.getKey()) >= 0) {
                newValue = false;
            }
            leaf.put(entry.getKey(), new Pair<>(entry.getValue(), false));
            checkAndSplit(stack);
            if (newValue) {
                size.getAndIncrement();
            } else {
                System.out.println("Update: " + entry.getKey());
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
            root = new BPlusTreeBranch<K>(comparator);
            // Ensure next and previous are set.
            ((BPlusTreeBranch<K>) root).put(key, split);
            return;
        }
        // Pop the parent... This is to enable linking, then we will put it back!
        BPlusTreeBranch<K> parent = (BPlusTreeBranch<K>) parents.peek();
        parent.put(key, split);
        // Check and split again!
        checkAndSplit(parents);
    }

    @SuppressWarnings("unchecked")
    private BPlusTreeLeaf<K, V> findLeafForGet(K key) {
        Node<K, ?> node = root;
        while (BPlusTreeBranch.class.isAssignableFrom(node.getClass())) {
            int pos = (node).findNearest(key);
            K found = node.getKeys().get(pos);
            Node[] children = ((BPlusTreeBranch<K>) node).get(found);
            int cmp = comparator.compare(found, key);
            if (cmp <= 0) {
                node = children[1];
            } else {
                node = children[0];
            }
        }
        return (BPlusTreeLeaf<K, V>) node;
    }

    @Override
    protected BTreeEntry<K, V> getEntry(K key) {
        lock.readLock().lock();
        try {
            BPlusTreeLeaf<K, V> leaf = findLeafForGet(key);
            Pair<V> value = leaf == null ? null : leaf.get(key);
            return value == null || value.isDeleted() ? null : new BTreeEntry<>(key, value.getValue(), false);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    protected V removeEntry(K key) {
        lock.writeLock().lock();
        try {
            BPlusTreeLeaf<K, V> leaf = findLeafForGet(key);
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
        root = new BPlusTreeLeaf<>(comparator);
    }

    // FIXME: This should be pushed up
    protected static class BPlusTreeBranch<K> extends Branch<K, Node> {

        public BPlusTreeBranch(Comparator<K> comparator) {
            super(comparator, new ArrayList<>());
        }

        public BPlusTreeBranch(Comparator<K> comparator, List<K> keys, List<Node> children) {
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
                if (BPlusTreeLeaf.class.isAssignableFrom(value[0].getClass())) {
                    ((BPlusTreeLeaf)value[0]).setPrevious((BPlusTreeLeaf)getChildren().get(nearest));
                    ((BPlusTreeLeaf)getChildren().get(nearest)).setNext((BPlusTreeLeaf)value[0]);
                    ((BPlusTreeLeaf)value[1]).setNext(((BPlusTreeLeaf)oldLeft).getNext());
                    if (((BPlusTreeLeaf)oldLeft).getNext() != null) {
                        ((BPlusTreeLeaf)oldLeft).getNext().setPrevious((BPlusTreeLeaf) value[1]);
                    }
                }
            } else {
                keys.add(nearest, key);
                Node<K, ?> oldLeft = getChildren().get(nearest);
                getChildren().add(nearest, value[0]);
                getChildren().set(nearest + 1, value[1]);
                if (BPlusTreeLeaf.class.isAssignableFrom(value[0].getClass())) {
                    ((BPlusTreeLeaf)value[0]).setPrevious(((BPlusTreeLeaf)oldLeft).getPrevious());
                    BPlusTreeLeaf previous = ((BPlusTreeLeaf) oldLeft).getPrevious();
                    if (previous != null) {
                        previous.setNext((BPlusTreeLeaf)value[0]);
                    }
                    BPlusTreeLeaf next = ((BPlusTreeLeaf)oldLeft).getNext();
                    ((BPlusTreeLeaf)value[1]).setNext(next);
                    if (next != null) {
                        next.setPrevious((BPlusTreeLeaf)value[1]);
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
            BPlusTreeBranch<K> left = new BPlusTreeBranch<>(comparator,
                    keys.subList(0, mid), getChildren().subList(0, mid + 1));
            BPlusTreeBranch<K> right = new BPlusTreeBranch<>(comparator,
                    keys.subList(mid + 1, keys.size()), getChildren().subList(mid + 1, getChildren().size()));
            return new Node[]{left, right};
        }
    }

    protected static class BPlusTreeLeaf<K, V> extends Leaf<K, V, BPlusTreeLeaf<K, V>> {

        private BPlusTreeLeaf<K, V> previous, next;

        public BPlusTreeLeaf(Comparator<K> comparator) {
            super(comparator);
        }

        public BPlusTreeLeaf(Comparator<K> comparator, BPlusTreeLeaf<K, V> previous, BPlusTreeLeaf<K, V> next) {
            super(comparator);
            this.previous = previous;
            this.next = next;
        }

        public BPlusTreeLeaf(Comparator<K> comparator, List<K> keys, List<Pair<V>> values) {
            super(comparator, keys, values);
        }

        public BPlusTreeLeaf(Comparator<K> comparator, List<K> keys, List<Pair<V>> values,
                             BPlusTreeLeaf<K, V> previous, BPlusTreeLeaf<K, V> next) {
            super(comparator, keys, values);
            this.previous = previous;
            this.next = next;
        }

        public BPlusTreeLeaf<K, V> getPrevious() {
            return previous;
        }

        public void setPrevious(BPlusTreeLeaf<K, V> previous) {
            this.previous = previous;
        }

        public BPlusTreeLeaf<K, V> getNext() {
            return next;
        }

        public void setNext(BPlusTreeLeaf<K, V> next) {
            this.next = next;
        }

        // TODO: If this works, then promote
        @Override
        @SuppressWarnings("unchecked")
        public Node<K, Pair<V>>[] split() {
            int mid = keys.size() - 1 >>> 1;
            BPlusTreeLeaf<K, V> left = newLeaf(keys.subList(0, mid), values.subList(0, mid));
            BPlusTreeLeaf<K, V> right = newLeaf(keys.subList(mid, keys.size()), values.subList(mid, values.size()));
            left.setNext(right);
            right.setPrevious(left);
            return new Node[]{left, right};
        }

        @Override
        protected BPlusTreeLeaf<K, V> newLeaf() {
            return new BPlusTreeLeaf<>(comparator);
        }

        @Override
        protected BPlusTreeLeaf<K, V> newLeaf(List<K> keys, List<Pair<V>> values) {
            return new BPlusTreeLeaf<>(comparator, keys, values);
        }
    }

    private class BTreeMapIterator implements Iterator<Entry<K, V>> {

        private final K to;
        private final boolean toInclusive, excludeDeleted;
        private final Comparator<K> comparator;
        // Iteration...
        private final AtomicInteger leafPos = new AtomicInteger();
        private final AtomicLong counter = new AtomicLong();
        private BPlusTreeLeaf<K, V> leaf;

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
            while (BPlusTreeBranch.class.isAssignableFrom(node.getClass())) {
                node = ((BPlusTreeBranch<K>) node).getChildren().get(0);
            }
            leaf = (BPlusTreeLeaf<K, V>) node;
        }

        @SuppressWarnings("unchecked")
        private void findLeaf(K key) {
            Node<K, ?> node = root;
            while (BPlusTreeBranch.class.isAssignableFrom(node.getClass())) {
                int pos = (node).findNearest(key);
                K found = node.getKeys().get(pos);
                // If key is higher than found, then we shift right, otherwise we shift left
                if (comparator.compare(key, found) >= 0) {
                    node = ((BPlusTreeBranch<K>) node).getChildren().get(pos + 1);
                } else {
                    node = ((BPlusTreeBranch<K>) node).getChildren().get(pos);
                }
            }
            leaf = (BPlusTreeLeaf<K, V>) node;
        }

        @SuppressWarnings("unchecked")
        private void advance() {
            // If the leaf position is still less than the size of the leaf, then we don't advance.
            // If the leaf position is greater than or equal to the size of the leaf, then we advance to the next leaf.
            if (leaf != null && leafPos.get() < leaf.getValues().size()) {
                return; // nothing to see here
            }

            // Move to the next leaf...
            BPlusTreeLeaf<K, V> oldLeaf = leaf;
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
        private BPlusTreeLeaf<K, V> leaf;

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
            while (BPlusTreeBranch.class.isAssignableFrom(node.getClass())) {
                node = ((BPlusTreeBranch<K>) node).getChildren().get(((BPlusTreeBranch<K>) node).getChildren().size() - 1);
            }
            leaf = (BPlusTreeLeaf<K, V>) node;
            leafPos.set(leaf.getKeys().size() - 1);
        }

        @SuppressWarnings("unchecked")
        private void findLeaf(K key) {
            Node<K, ?> node = root;
            while (BPlusTreeBranch.class.isAssignableFrom(node.getClass())) {
                int pos = (node).findNearest(key);
                K found = node.getKeys().get(pos);
                // If key is higher than found, then we shift right, otherwise we shift left
                if (comparator.compare(key, found) >= 0) {
                    node = ((BPlusTreeBranch<K>) node).getChildren().get(pos);
                } else {
                    node = ((BPlusTreeBranch<K>) node).getChildren().get(pos - 1);
                }
            }
            leaf = (BPlusTreeLeaf<K, V>) node;
            leafPos.set(leaf.getKeys().size() - 1);
        }

        @SuppressWarnings("unchecked")
        private void advance() {
            if (leaf != null && leafPos.get() >= 0) {
                return; // nothing to see here
            }

            leaf = ((BPlusTreeLeaf<K, V>) leaf).getPrevious();
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
