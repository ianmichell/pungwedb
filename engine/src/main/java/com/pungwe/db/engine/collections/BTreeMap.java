package com.pungwe.db.engine.collections;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ian on 07/07/2016.
 */
public class BTreeMap<K,V> extends AbstractBTreeMap<K,V> {

    protected final int maxKeysPerBlock;
    protected Block<? extends Node> root;
    protected final AtomicLong size = new AtomicLong();

    public BTreeMap(Comparator<K> comparator, int maxKeysPerBlock) {
        super(comparator);
        root = new Block<>(Leaf.class);
        this.maxKeysPerBlock = maxKeysPerBlock;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected BTreeEntry<K, V> putEntry(Entry<? extends K, ? extends V> entry) {
        Block<? extends Node<K>> block = root;

        Stack<Block<Branch<? extends Node<K>>>> parents = new Stack<>();
        while (Branch.class.isAssignableFrom(block.getType())) {
            parents.push((Block<Branch<? extends Node<K>>>) block);
            Branch<?> branch = (Branch<?>)block.getNearest(entry.getKey());
            int cmp = comparator.compare(branch.getKey(), entry.getKey());
            if (cmp < 0) {
                block = branch.getLeft();
            } else {
                block = branch.getRight();
            }
        }

        Leaf leaf = new Leaf(entry.getKey(), entry.getValue(), false);
        boolean insert = block.getNode(entry.getKey()) != null;
        ((Block<Leaf>)block).putNode(leaf);

        if (block.size() > maxKeysPerBlock) {
            Block<Branch<?>> parent = parents.isEmpty() ? null : parents.pop();
            Branch<?> branch = block.split();
            if (parent == null) {
                parent = new Block<>((Class<Branch<?>>)branch.getClass());
                parents.push(parent);
            }
            parent.putNode(branch);
            checkAndSplit(parents);
        }
        if (insert) {
            size.getAndIncrement();
        }
        return leaf;
    }

    @SuppressWarnings("unchecked")
    private void checkAndSplit(Stack<Block<Branch<? extends Node<K>>>> parents) {
        // Get the current parent
        Block<Branch<? extends Node<K>>> check = parents.pop();
        if (check.size() <= maxKeysPerBlock) {
            // Do nothing
            return;
        }
        Branch<? extends Node<K>> branch = check.split();
        Block<Branch<? extends Node<K>>> parent = null;
        // New root
        if (parents.isEmpty()) {
            parent = new Block<>((Class<Branch<? extends Node<K>>>)branch.getClass());
            parent.putNode(branch);
            root = parent;
            return;
        }
        // Peek the parent from the stack
        parent = parents.peek();
        parent.putNode(branch);
        // Check and split again...
        checkAndSplit(parents);
    }

    @Override
    protected BTreeEntry<K, V> getEntry(K key) {
        Block<? extends Node<K>> block = root;
        while (Branch.class.isAssignableFrom(block.getType())) {
            Branch<?> branch = (Branch<?>)block.getNearest(key);
            int cmp = comparator.compare(branch.getKey(), key);
            if (cmp < 0) {
                block = branch.getLeft();
            } else {
                block = branch.getRight();
            }
        }
        return (Leaf)block.getNode(key);
    }

    @Override
    protected V removeEntry(K key) {
        return null;
    }

    @Override
    protected long sizeLong() {
        return size.get();
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

    }

    private final class Block<T extends Node<K>> implements Iterable<T> {

        private final Class<T> type;
        private final Branch<? extends T> parent;
        // Array list will grow by block.
        private final List<T> entries;

        public Block(Class<T> clz) {
            entries = new ArrayList<>();
            parent = null;
            type = clz;
        }

        public Block(Class<T> clz, List<T> entries) {
            this.entries = entries;
            this.parent = null;
            this.type = clz;
        }

        public Block(Class<T> clz, List<T> entries, Branch<? extends T> parent) {
            this.entries = entries;
            this.parent = parent;
            this.type = clz;
        }

        @SuppressWarnings("unchecked")
        public T putNode(T node) {
            // FIXME: Ensure that we change left and right on the lesser and greater nodes
            int pos = findPosition(node.getKey());
            boolean insert = pos < 0;
            pos =  pos < 0 ? -(pos + 1) : pos;
            // Insert operation we add the node to the list, otherwise we set it's position
            if (insert) {
                entries.add(pos, node);
            } else {
                entries.set(pos, node);
            }
            // If this is a branch block, we need to update the left and the right
            if (Leaf.class.isAssignableFrom(type)) {
                return node;
            }

            Branch<?> branch = (Branch<?>)node;

            int leftPos = pos - 1;
            int rightPos = pos + 1;
            if (leftPos >= 0) {
                Branch<?> left = (Branch<?>)entries.get(leftPos);
                left.setRight(branch.getLeft());
            }
            if (rightPos < entries.size()) {
                Branch<?> right = (Branch<?>)entries.get(rightPos);
                right.setLeft(branch.getRight());
            }
            return node;
        }

        public Branch<T> split() {
            int mid = (entries.size() - 1) >>> 1;
            K key = entries.get(mid).getKey();
            List<T> leftEntries = entries.subList(0, mid + 1);
            List<T> rightEntries = entries.subList(mid, entries.size());
            // we are splitting this block, so "type" is the same as this block
            return new Branch<T>(key, new Block<>(type, leftEntries), new Block<>(type, rightEntries));
        }

        public T getNode(K key) {
            int pos = findPosition(key);
            return pos < 0 ? null : entries.get(pos);
        }

        public T getNearest(K key) {
            int pos = findPosition(key);
            pos = pos < 0 ? -(pos + 1) : pos;
            if (pos < entries.size()) {
                return entries.get(pos);
            }
            return entries.get(pos - 1);
        }

        public Class<T> getType() {
            return type;
        }

        public Branch<? extends T> getParent() {
            return parent;
        }

        public List<T> getEntries() {
            return entries;
        }

        @Override
        public Iterator<T> iterator() {
            return entries.iterator();
        }

        public int size() {
            return entries.size();
        }

        private int findPosition(K key) {
            return Collections.binarySearch(entries, new KeyNode(key), (o1, o2) -> comparator.compare(o1.getKey(),
                    o2.getKey()));
        }
    }

    protected interface Node<K> extends Comparable<Node<K>> {

        K getKey();
        Comparator<K> comparator();

        @Override
        default int compareTo(Node<K> o) {
            return comparator().compare(getKey(), o.getKey());
        }
    }

    private class KeyNode implements Node<K> {

        private final K key;

        public KeyNode(K key) {
            this.key = key;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public Comparator<K> comparator() {
            return comparator;
        }
    }

    protected class Branch<T extends Node<K>> implements Node<K> {

        private final K key;
        private Block<T> left;
        private Block<T> right;

        public Branch(K key, Block<T> left, Block<T> right) {
            this.key = key;
            this.left = left;
            this.right = right;
        }

        @Override
        public Comparator<K> comparator() {
            return comparator;
        }

        @Override
        public K getKey() {
            return key;
        }

        public Block<T> getLeft() {
            return left;
        }

        public Block<T> getRight() {
            return right;
        }

        @SuppressWarnings("unchecked")
        public void setLeft(Block<?> left) {
            this.left = (Block<T>)left;
        }

        @SuppressWarnings("unchecked")
        public void setRight(Block<?> right) {
            this.right = (Block<T>)right;
        }
    }

    protected class Leaf extends BTreeEntry<K,V> implements Node<K> {

        public Leaf(K key, V value, boolean deleted) {
            super(key, value, deleted);
        }

        @Override
        public Comparator<K> comparator() {
            return comparator;
        }
    }

    private class EntryIterator implements Iterator<Leaf> {

        private final K from, to;
        private final boolean fromInclusive, toInclusive;
        private final Comparator<K> comparator;

        public EntryIterator(K from, K to, boolean fromInclusive, boolean toInclusive, Comparator<K> comparator) {
            this.from = from;
            this.to = to;
            this.fromInclusive = fromInclusive;
            this.toInclusive = toInclusive;
            this.comparator = comparator;
        }

        private void pointToStart() {
            
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Leaf next() {
            return null;
        }
    }
}
