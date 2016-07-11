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

/**
 * Created by ian on 07/07/2016.
 */
public class BTreeMap<K, V> extends AbstractBTreeMap<K, V> {

    protected final int maxKeysPerNode;
    protected final AtomicLong size = new AtomicLong();
    protected Node<K, ?> root;

    public BTreeMap(Comparator<K> comparator, int maxKeysPerNode) {
        super(comparator);
        this.maxKeysPerNode = maxKeysPerNode;
        this.root = new Leaf<>(comparator);
    }

    @Override
    protected Node<K, ?> rootNode() {
        return root;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected BTreeEntry<K, V> putEntry(Entry<? extends K, ? extends V> entry) {
        if (entry == null || entry.getKey() == null) {
            return null;
        }
        Stack<Node<K, ?>> stack = new Stack<>();
        Node<K, ?> node = root;
        stack.push(node);
        while (MutableBranch.class.isAssignableFrom(node.getClass())) {
            int pos = node.findNearest(entry.getKey());
            K found = node.getKeys().get(pos);
            Node[] children = ((MutableBranch<K>) node).get(found);
            if (comparator.compare(found, entry.getKey()) <= 0) {
                // Swing to the right
                node = children[1];
            } else {
                node = children[0];
            }
            stack.push(node);
        }
        Leaf<K, V> leaf = (Leaf<K, V>) node;
        boolean newValue = true;
        if (leaf.findPosition(entry.getKey()) >= 0) {
            newValue = false;
        }
        leaf.put(entry.getKey(), new Pair<V>(entry.getValue(), false));
        checkAndSplit(stack);
        size.getAndIncrement();
        return new BTreeEntry<>(entry.getKey(), entry.getValue(), false);
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
            root = new MutableBranch<>(comparator);
            ((MutableBranch<K>) root).put(key, split);
            return;
        }
        // Peek, don't pop
        MutableBranch<K> parent = (MutableBranch<K>) parents.peek();
        parent.put(key, split);
        // Check and split again!
        checkAndSplit(parents);
    }

    @Override
    protected BTreeEntry<K, V> getEntry(K key) {
        Leaf leaf = findLeafForGet(key);
        Pair<V> value = leaf == null ? null : leaf.get(key);
        return value == null || value.isDeleted() ? null : new BTreeEntry<K, V>(key, value.getValue(), false);
    }

    @Override
    protected V removeEntry(K key) {
        Leaf leaf = findLeafForGet(key);
        Pair<V> pair = leaf.get(key);
        if (pair == null) {
            return null;
        }
        pair.setDeleted(true);
        size.decrementAndGet();
        return pair.getValue();
    }

    @Override
    protected long sizeLong() {
        return size.get();
    }

    @SuppressWarnings("unchecked")
    private Leaf findLeafForGet(K key) {
        Node<K, ?> node = root;
        while (MutableBranch.class.isAssignableFrom(node.getClass())) {
            int pos = (node).findNearest(key);
            K found = node.getKeys().get(pos);
            Node[] children = ((MutableBranch<K>) node).get(found);
            int cmp = comparator.compare(found, key);
            if (cmp <= 0) {
                node = children[1];
            } else {
                node = children[0];
            }
        }
        return (Leaf<K, V>) node;
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
    public void clear() {
        Iterator<Entry<K, V>> it = iterator();
        while (it.hasNext()) {
            it.remove();
        }
    }

    private static class MutableBranch<K> extends Branch<K, Node> {

        public MutableBranch(Comparator<K> comparator) {
            super(comparator, new ArrayList<>());
        }

        @Override
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
            if (cmp == 0) {
                keys.set(nearest, key);
                getChildren().set(nearest, value[0]);
                getChildren().set(nearest + 1, value[1]);
            } else if (cmp < 0) {
                keys.add(nearest + 1, key);
                getChildren().set(nearest + 1, value[0]);
                getChildren().add(nearest + 2, value[1]);
            } else {
                keys.add(nearest, key);
                getChildren().add(nearest, value[0]);
                getChildren().set(nearest + 1, value[1]);
            }
        }

        @Override
        public Node<K, Node[]>[] split() {
            int mid = keys.size() - 1 >>> 1;
            MutableBranch<K> left = new MutableBranch<K>(comparator);
            left.keys = new ArrayList<>();
            left.keys.addAll(keys.subList(0, mid));
            left.getChildren().addAll(getChildren().subList(0, mid + 1));
            MutableBranch<K> right = new MutableBranch<K>(comparator);
            right.keys = new ArrayList<K>();
            right.keys.addAll(keys.subList(mid + 1, keys.size()));
            right.getChildren().addAll(getChildren().subList(mid + 1, getChildren().size()));
            return new Node[]{left, right};
        }

        @Override
        public Node[] get(K key) {
            int pos = findNearest(key);
            if (pos > keys.size()) {
                pos -= 1;
            }
            return new Node[]{getChildren().get(pos), getChildren().get(pos + 1)};
        }
    }

    private class BTreeMapIterator implements Iterator<Entry<K, V>> {

        private final K to;
        private final boolean toInclusive, excludeDeleted;
        private final Comparator<K> comparator;
        // Iteration...
        private final Stack<MutableBranch<K>> stack = new Stack<>();
        private final Stack<AtomicInteger> stackPos = new Stack<>();
        private final AtomicInteger leafPos = new AtomicInteger();
        private Leaf<K, V> leaf;
        private Pair<V> last;

        private BTreeMapIterator(Comparator<K> comparator, K from, boolean fromInclusive, K to, boolean toInclusive) {
            this(comparator, from, fromInclusive, to, toInclusive, true);
        }

        private BTreeMapIterator(Comparator<K> comparator, K from, boolean fromInclusive, K to, boolean toInclusive,
                                 boolean excludeDeleted) {
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
                    stack.clear();
                    stackPos.clear();
                }
            }
            return leaf != null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Entry<K, V> next() {
            if (!hasNext()) {
                return null;
            }
            K key = leaf.getKeys().get(leafPos.get());
            Pair<V> child = leaf.getValues().get(leafPos.getAndIncrement());
            advance();
            return new BTreeEntry<>(key, child.getValue(), child.isDeleted());
        }

        @Override
        public void remove() {
            last.setDeleted(true);
            last = null;
            // Decrement
            size.decrementAndGet();
        }

        @SuppressWarnings("unchecked")
        private void pointToStart() {
            Node<K, ?> node = root;
            while (MutableBranch.class.isAssignableFrom(node.getClass())) {
                stack.push((MutableBranch<K>) node);
                stackPos.push(new AtomicInteger(1));
                node = ((MutableBranch<K>) node).getChildren().get(0);
            }
            leaf = (Leaf<K, V>) node;
        }

        @SuppressWarnings("unchecked")
        private void findLeaf(K key) {
            Node<K, ?> node = root;
            while (MutableBranch.class.isAssignableFrom(node.getClass())) {
                stack.push((MutableBranch<K>) node);
                int pos = (node).findNearest(key);
                stackPos.push(new AtomicInteger(pos + 1));
                K found = node.getKeys().get(pos);
                // If key is higher than found, then we shift right, otherwise we shift left
                if (comparator.compare(key, found) >= 0) {
                    node = ((MutableBranch<K>) node).getChildren().get(pos + 1);
                    // Increment the stack position, if we lean right. This is to ensure that we don't repeat.
                    stackPos.peek().getAndIncrement();
                } else {
                    node = ((MutableBranch<K>) node).getChildren().get(pos);
                }
            }
            leaf = (Leaf<K, V>) node;
        }

        @SuppressWarnings("unchecked")
        private void advance() {
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

            MutableBranch<K> parent = stack.peek(); // get the immediate parent

            int pos = stackPos.peek().getAndIncrement(); // get the immediate parent position.
            if (pos < parent.getChildren().size()) {
                Node<K, ?> child = parent.getChildren().get(pos);
                if (Leaf.class.isAssignableFrom(child.getClass())) {
                    leaf = (Leaf<K, V>) child;
                    leafPos.set(0);
                } else {
                    stack.push((MutableBranch<K>) child);
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

    private class ReverseBTreeMapIterator implements Iterator<Entry<K, V>> {

        private final K to;
        private final boolean toInclusive, excludeDeleted;
        private final Comparator<K> comparator;
        // Iteration...
        private final Stack<MutableBranch<K>> stack = new Stack<>();
        private final Stack<AtomicInteger> stackPos = new Stack<>();
        private final AtomicInteger leafPos = new AtomicInteger();
        private Leaf<K, V> leaf;

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
                    stack.clear();
                    stackPos.clear();
                }
            }
            return leaf != null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Entry<K, V> next() {
            if (!hasNext()) {
                return null;
            }
            K key = leaf.getKeys().get(leafPos.get());
            Pair<V> child = leaf.getValues().get(leafPos.getAndDecrement());
            advance();
            return new BTreeEntry<>(key, child.getValue(), child.isDeleted());
        }

        @SuppressWarnings("unchecked")
        private void pointToStart() {
            Node<K, ?> node = root;
            while (MutableBranch.class.isAssignableFrom(node.getClass())) {
                stack.push((MutableBranch<K>) node);
                stackPos.push(new AtomicInteger(((MutableBranch<K>) node).getChildren().size() - 2));
                node = ((MutableBranch<K>) node).getChildren().get(((MutableBranch<K>) node).getChildren().size() - 1);
            }
            leaf = (Leaf<K, V>) node;
            leafPos.set(leaf.getKeys().size() - 1);
        }

        @SuppressWarnings("unchecked")
        private void findLeaf(K key) {
            Node<K, ?> node = root;
            while (MutableBranch.class.isAssignableFrom(node.getClass())) {
                stack.push((MutableBranch<K>) node);
                int pos = (node).findNearest(key);
                stackPos.push(new AtomicInteger(pos - 1));
                K found = node.getKeys().get(pos);
                // If key is higher than found, then we shift right, otherwise we shift left
                if (comparator.compare(key, found) >= 0) {
                    node = ((MutableBranch<K>) node).getChildren().get(pos);
                } else {
                    node = ((MutableBranch<K>) node).getChildren().get(pos - 1);
                    // Increment the stack position, if we lean right. This is to ensure that we don't repeat.
                    stackPos.peek().getAndDecrement();
                }
            }
            leaf = (Leaf<K, V>) node;
            leafPos.set(leaf.getKeys().size() - 1);
        }

        @SuppressWarnings("unchecked")
        private void advance() {
            if (leaf != null && leafPos.get() >= 0) {
                return; // nothing to see here
            }

            leaf = null;
            leafPos.set(-1); // reset to 0

            if (stack.isEmpty()) {
                return; // nothing to see here
            }

            MutableBranch<K> parent = stack.peek(); // get the immediate parent

            int pos = stackPos.peek().getAndDecrement(); // get the immediate parent position.
            if (pos >= 0) {
                Node<K, ?> child = parent.getChildren().get(pos);
                if (Leaf.class.isAssignableFrom(child.getClass())) {
                    leaf = (Leaf<K, V>) child;
                    leafPos.set(leaf.getKeys().size() - 1);
                } else {
                    stack.push((MutableBranch<K>) child);
                    stackPos.push(new AtomicInteger(((MutableBranch<K>) child).getChildren().size() - 1));
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
