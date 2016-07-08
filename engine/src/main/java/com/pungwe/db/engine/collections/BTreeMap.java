package com.pungwe.db.engine.collections;

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
        while (Branch.class.isAssignableFrom(node.getClass())) {
            int pos = node.findNearest(entry.getKey());
            K found = node.getKeys().get(pos);
            Node[] children = ((Branch<K>) node).get(found);
            if (comparator.compare(found, entry.getKey()) <= 0) {
                // Swing to the left
                node = children[0];
            } else {
                node = children[1];
            }
            stack.push(node);
        }
        Leaf<K, V> leaf = (Leaf<K, V>) node;
        leaf.put(entry.getKey(), new Pair<V>(entry.getValue(), false));
        checkAndSplit(stack);
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
            root = new Branch<>(comparator);
            ((Branch<K>)root).put(key, split);
            return;
        }
        // Peek, don't pop
        Branch<K> parent = (Branch<K>)parents.peek();
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
        while (Branch.class.isAssignableFrom(node.getClass())) {
            int pos = (node).findNearest(key);
            node = ((Branch<K>) node).getChildren().get(pos);
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

    private static class Branch<K> extends Node<K, Node[]> {

        List<Node> children = new ArrayList<>();

        public Branch(Comparator<K> comparator) {
            super(comparator);
        }

        @Override
        public void put(K key, Node[] value) {
            int nearest = findNearest(key);
            K found = keys.get(nearest);
            int cmp = comparator.compare(found, key);
            // Already exists, so replace it
            if (cmp == 0) {
                keys.set(nearest, key);
                children.set(nearest, value[0]);
                children.set(nearest + 1, value[1]);
            } else if (cmp < 0) {
                keys.add(nearest + 1, key);
                children.set(nearest + 1, value[0]);
                children.add(nearest + 2, value[1]);
            } else {
                keys.add(nearest, key);
                children.add(nearest, value[0]);
                children.set(nearest + 1, value[1]);
            }
        }

        @Override
        public Node<K, Node[]>[] split() {
            int mid = keys.size() - 1 >>> 1;
            Branch<K> left = new Branch<K>(comparator);
            left.keys = keys.subList(0, mid);
            left.children = children.subList(0, mid + 1);
            Branch<K> right = new Branch<K>(comparator);
            right.keys = keys.subList(mid + 1, keys.size());
            right.children = children.subList(mid + 1, children.size());
            return new Node[] { left, right };
        }

        public List<Node> getChildren() {
            return children;
        }

        @Override
        public Node[] get(K key) {
            int pos = findNearest(key);
            return new Node[]{children.get(pos), children.get(pos + 1)};
        }
    }

    private static class Leaf<K, V> extends Node<K, Pair<V>> {

        private List<Pair<V>> values = new ArrayList<>();

        public Leaf(Comparator<K> comparator) {
            super(comparator);
        }

        @Override
        public void put(K key, Pair<V> value) {
            int nearest = findNearest(key);
            K found = keys.get(nearest);
            int cmp = comparator.compare(found, key);
            if (cmp == 0) {
                keys.set(nearest, key);
                values.set(nearest, value);
            } else if (cmp < 0) {
                // found is lower
                keys.add(nearest + 1, key);
                values.add(nearest + 1, value);
            } else {
                keys.add(nearest, key);
                values.add(nearest, value);
            }
        }

        @Override
        public Pair<V> get(K key) {
            int pos = findPosition(key);
            return pos < 0 ? null : values.get(pos);
        }

        public List<Pair<V>> getValues() {
            return values;
        }

        @Override
        public Node<K, Pair<V>>[] split() {
            int mid = keys.size() - 1 >>> 1;
            Leaf<K, V> left = new Leaf<>(comparator);
            left.keys = keys.subList(0, mid);
            left.values = values.subList(0, mid);
            Leaf<K, V> right = new Leaf<>(comparator);
            right.keys = keys.subList(mid + 1, keys.size());
            right.values = values.subList(mid + 1, values.size());

            return new Node[] { left, right };
        }
    }

    private static class Pair<V> {
        public V value;
        public boolean deleted;

        public Pair(V value, boolean deleted) {
            this.value = value;
            this.deleted = deleted;
        }

        public V getValue() {
            return value;
        }

        public void setValue(V value) {
            this.value = value;
        }

        public boolean isDeleted() {
            return deleted;
        }

        public void setDeleted(boolean deleted) {
            this.deleted = deleted;
        }
    }

    private class BTreeMapIterator implements Iterator<Entry<K, V>> {

        private final K to;
        private final boolean toInclusive, excludeDeleted;
        private final Comparator<K> comparator;
        // Iteration...
        private final Stack<Branch<K>> stack = new Stack<>();
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
                if (comp > 0) {
                    leafPos.set(pos);
                } else if (comp == 0) {
                    leafPos.set(fromInclusive ? pos : pos + 1);
                } else if (comp < 0) {
                    leafPos.set(pos);
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
            if (leafPos.get() >= leaf.getValues().size()) {
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
            while (Branch.class.isAssignableFrom(node.getClass())) {
                stack.push((Branch<K>) node);
                stackPos.push(new AtomicInteger(1));
                node = ((Branch<K>) node).getChildren().get(0);
            }
            leaf = (Leaf<K, V>) node;
        }

        @SuppressWarnings("unchecked")
        private void findLeaf(K key) {
            Node<K, ?> node = root;
            while (Branch.class.isAssignableFrom(node.getClass())) {
                stack.push((Branch<K>) node);
                int pos = (node).findNearest(key);
                stackPos.push(new AtomicInteger(pos + 1));
                node = ((Branch<K>) node).getChildren().get(pos);
            }
            leaf = (Leaf<K, V>) node;
        }

        @SuppressWarnings("unchecked")
        private void advance() {
            if (leaf != null && leafPos.get() < leaf.getValues().size()) {
                return; // nothing to see here
            }

            leaf = null;
            leafPos.set(-1); // reset to 0

            if (stack.isEmpty()) {
                return; // nothing to see here
            }

            Branch<K> parent = stack.peek(); // get the immediate parent

            int pos = stackPos.peek().getAndIncrement(); // get the immediate parent position.
            if (pos < parent.getChildren().size()) {
                Node<K, ?> child = parent.getChildren().get(pos);
                if (Leaf.class.isAssignableFrom(child.getClass())) {
                    leaf = (Leaf<K, V>) child;
                    leafPos.set(0);
                } else {
                    stack.push((Branch<K>) child);
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
        private final Stack<Branch<K>> stack = new Stack<>();
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
                if (comp > 0) {
                    leafPos.set(pos);
                } else if (comp == 0) {
                    leafPos.set(fromInclusive ? pos : pos - 1);
                } else if (comp < 0) {
                    leafPos.set(pos);
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
            if (leafPos.get() >= 0) {
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
            while (Branch.class.isAssignableFrom(node.getClass())) {
                stack.push((Branch<K>) node);
                stackPos.push(new AtomicInteger(((Branch<K>) node).getChildren().size() - 2));
                node = ((Branch<K>) node).getChildren().get(((Branch<K>) node).getChildren().size() - 1);
            }
            leaf = (Leaf<K, V>) node;
        }

        @SuppressWarnings("unchecked")
        private void findLeaf(K key) {
            Node<K, ?> node = root;
            while (Branch.class.isAssignableFrom(node.getClass())) {
                stack.push((Branch<K>) node);
                int pos = (node).findNearest(key);
                stackPos.push(new AtomicInteger(pos - 1));
                node = ((Branch<K>) node).getChildren().get(pos);
            }
            leaf = (Leaf<K, V>) node;
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

            Branch<K> parent = stack.peek(); // get the immediate parent

            int pos = stackPos.peek().getAndDecrement(); // get the immediate parent position.
            if (pos >= 0) {
                Node<K, ?> child = parent.getChildren().get(pos);
                if (Leaf.class.isAssignableFrom(child.getClass())) {
                    leaf = (Leaf<K, V>) child;
                    leafPos.set(0);
                } else {
                    stack.push((Branch<K>) child);
                    stackPos.push(new AtomicInteger(((Branch<K>) child).getChildren().size()));
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
