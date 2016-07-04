package com.pungwe.db.engine.collections;

import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.registry.SerializerRegistry;
import com.pungwe.db.engine.io.store.Store;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ian on 02/07/2016.
 */
public class BTreeMap2<K, V> extends BaseMap<K, V> {

    private final BTreeNodeSerializer nodeSerializer;
    private final int maxSize, maxKeys;
    private final Store store;
    private final AtomicLong rootPointer = new AtomicLong(-1);
    protected final AtomicLong size = new AtomicLong();

    public BTreeMap2(Store store, Comparator<K> keyComparator, Serializer keySerializer, Serializer valueSerializer,
                     int maxKeys, int maxSize, long rootPointer) {
        super(keyComparator);
        this.store = store;
        this.maxKeys = maxKeys;
        this.maxSize = maxSize;
        this.rootPointer.set(rootPointer);
        nodeSerializer = new BTreeNodeSerializer(keySerializer, valueSerializer);
        if (!SerializerRegistry.getIntance().hasSerializer(nodeSerializer.getKey())) {
            SerializerRegistry.getIntance().register(nodeSerializer);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Entry<K, V> getEntry(K key) {
        lock.readLock().lock();
        try {
            Leaf<K, V> leaf = findLeaf(key);
            if (leaf != null && leaf.hasKey(key, (Comparator<K>) comparator())) {
                return new BaseMapEntry<>(key, leaf.getValue(key, (Comparator<K>) comparator()), this);
            }
            return null;
        } catch (IOException ex) {
            // Do some logging here... Otherwise return nothing if there is an error
            // We should probably have a run time exception of sorts.
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Entry<K, V> putEntry(K key, V value, boolean replace) {
        lock.writeLock().lock();
        try {
            Node[] parents = new Node[1];
            long[] pointers = new long[1];
            // Fetch the root node
            Node<K> node = rootPointer.get() > -1 ? (Node<K>) store.get(rootPointer.get(), nodeSerializer)
                    : new Leaf<>();
            parents[0] = node;
            pointers[0] = rootPointer.get();
            int pos = 1;
            while (Branch.class.isAssignableFrom(node.getClass())) {
                long position = ((Branch<K>) node).findChild(key, (Comparator<K>) comparator());
                node = (Node<K>) store.get(position, nodeSerializer);
                if (pos == pointers.length) {
                    pointers = Arrays.copyOf(pointers, pointers.length + 1);
                    parents = Arrays.copyOf(parents, parents.length + 1);
                }
                // Add the node to the arrays
                pointers[pos] = position;
                parents[pos] = node;
                pos++;
            }

            Leaf<K, V> leaf = (Leaf<K, V>) node;
            boolean exists = leaf.hasKey(key, (Comparator<K>) comparator());
            if (!exists && sizeLong() >= maxSize && maxSize > -1) {
                throw new IllegalArgumentException("BTree Full");
            }
            leaf.put(key, value, (Comparator<K>) comparator());
            if (maxKeys > 0 && leaf.keys.length > maxKeys) {
                // split
                split(parents, pointers);
            } else {
                // save
                save(parents, pointers);
            }

            if (!exists) {
                // increment size of the tree
                size.getAndIncrement();
            }
            return new BaseMapEntry<K, V>(key, value, this);
        } catch (IOException ex) {
            // FIXME: throw a runtime
            return null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long sizeLong() {
        return size.get();
    }

    private void save(Node<K>[] nodes, long[] pointers) throws IOException {
        for (int i = nodes.length - 1; i >= 0; i--) {
            long current = pointers[i];
            Node<K> node = nodes[i];
            Branch<K> parent = i - 1 < 0 ? null : (Branch<K>) nodes[i - 1];

            // If it's a new node we should add it
            long newPointer = current > -1 ? store.update(current, node, nodeSerializer) :
                    store.add(node, nodeSerializer);

            // If this is the root node, then update it to point at rootPointer...
            if (current == rootPointer.get() && newPointer != current) {
                rootPointer.set(newPointer);
            }

            // If the parent isn't null, then update the position of the child.
            if (parent != null && newPointer != current) {
                long[] children = parent.children;
                int idx = Arrays.binarySearch(children, current);
                if (idx >= 0 && idx < children.length) {
                    children[idx] = newPointer;
                }
            }
        }
    }

    private void split(Node<K>[] nodes, long[] pointers) throws IOException {
        long offset = pointers[pointers.length - 1];
        Node<K> node = nodes[pointers.length - 1];
        int mid = (node.keys.length - 1) >>> 1;
        K key = (K) node.keys[mid];
        Node<K> left = node.copyLeftSplit(mid);
        Node<K> right = node.copyRightSplit(mid);
        long[] children = new long[2];
        children[0] = store.update(offset, left, nodeSerializer);
        children[1] = store.add(right, nodeSerializer);

        if (pointers.length == 1) {
            Branch<K> newRoot = new Branch<>();
            newRoot.put(key, children, (Comparator<K>) comparator());
            rootPointer.set(store.add(newRoot, nodeSerializer));
            return;
        }

        Branch<K> parent = (Branch<K>) nodes[pointers.length - 2];
        parent.put(key, children, (Comparator<K>) comparator());
        if (parent.keys.length > maxKeys) {
            split(Arrays.copyOf(nodes, nodes.length - 1), Arrays.copyOf(pointers, pointers.length - 1));
        } else {
            save(Arrays.copyOf(nodes, nodes.length - 1), Arrays.copyOf(pointers, pointers.length - 1));
        }
    }

    @Override
    protected Iterator<Entry<K, V>> descendingIterator(Comparator<? super K> comparator, K low, boolean lowInclusive,
                                                       K high, boolean highInclusive) {
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Iterator<Entry<K, V>> iterator(Comparator<? super K> comparator, K low, boolean lowInclusive, K high,
                                             boolean highInclusive) {
        return new BTreeIterator<K, V>(this, (Comparator<K>) comparator, low, lowInclusive, high, highInclusive);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return false;
    }

    @Override
    public int size() {
        return Math.min((int) sizeLong(), Integer.MAX_VALUE);
    }

    @Override
    public void clear() {

    }

    @SuppressWarnings("unchecked")
    private Leaf<K, V> findLeaf(K key) throws IOException {
        Node<K> node = (Node<K>) store.get(rootPointer.get(), nodeSerializer);
        if (node == null) {
            return null;
        }
        while (Branch.class.isAssignableFrom(node.getClass())) {
            long position = ((Branch<K>) node).findChild(key, (Comparator<K>) comparator());
            node = (Node<K>) store.get(position, nodeSerializer);
        }
        return (Leaf<K, V>) node;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return null;
    }

    public static abstract class Node<K> {
        protected Object[] keys = new Object[0];

        protected int findPosition(K key, Comparator<K> comparator) {
            int low = 0;
            int high = keys.length - 1;
            return findPosition(key, low, high, comparator);
        }

        protected int findPosition(K key, int low, int high, Comparator<K> comparator) {

            if (keys.length == 0) {
                return 0;
            }

            K lowKey = (K) keys[low];
            K highKey = (K) keys[high];

            int highComp = comparator.compare(key, highKey);
            int lowComp = comparator.compare(key, lowKey);

            // Check high
            if (highComp < 0) {
                high--;
            } else if (highComp == 0) {
                return high;
            } else if (highComp > 0) {
                return high + 1;
            }

            // Check low
            if (lowComp <= 0) {
                return low;
            } else if (lowComp > 0) {
                low++;
            }

            if (low > high) {
                return high;
            }

            int mid = (low + high) >>> 1;
            K midKey = (K) keys[mid];
            int midComp = comparator.compare(key, midKey);

            // Check mid
            if (midComp > 0) {
                low = mid + 1;
            } else if (midComp == 0) {
                return mid;
            } else if (midComp < 0) {
                high = mid - 1;
            }

            return findPosition(key, low, high, comparator);
        }

        public abstract Node<K> copyLeftSplit(int mid);

        public abstract Node<K> copyRightSplit(int mid);
    }

    public final static class Branch<K> extends Node<K> {
        protected long[] children = new long[0];

        @SuppressWarnings("unchecked")
        public void put(K key, long[] leftRight, Comparator<K> comparator) {
            // Find the position of the new key
//            int idx = findPosition(key, comparator);
            if (keys.length == 0) {
                keys = new Object[1];
                keys[0] = key;
                children = leftRight;
                return;
            }

            int idx = findPosition(key, comparator);
            // If it's at the end then insert the key at the end
            if (idx >= keys.length) {
                keys = Arrays.copyOf(keys, keys.length + 1);
                children = Arrays.copyOf(children, children.length + 1);
                keys[keys.length - 1] = key;
                children[keys.length - 1] = leftRight[0];
                children[keys.length] = leftRight[1];
                return;
            }
            // Otherwise we need to figure out where we are going
            int cmp = comparator.compare((K)keys[idx], key);
            // If it's equal it's a replacement
            if (cmp == 0) {
                keys[idx] = key;
                children[idx] = leftRight[0];
                children[idx + 1] = leftRight[1];
            } else if (cmp < 0) {
                Object[] newKeys = Arrays.copyOf(keys, keys.length + 1);
                long[] newChildren = Arrays.copyOf(children, children.length + 1);
                System.arraycopy(keys, idx + 1, newKeys, idx + 2, keys.length - (idx + 1));
                System.arraycopy(children, idx + 1, newChildren, idx + 2, newChildren.length - (idx + 1));
                newKeys[idx + 1] = key;
                newChildren[idx + 1] = leftRight[0];
                newChildren[idx + 2] = leftRight[1];
                keys = newKeys;
                children = newChildren;
            } else {
                Object[] newKeys = Arrays.copyOf(keys, keys.length + 1);
                long[] newChildren = Arrays.copyOf(children, children.length + 1);
                System.arraycopy(keys, idx, newKeys, idx + 1, keys.length - (idx + 1));
                System.arraycopy(children, idx, newChildren, idx + 1, newChildren.length - (idx + 1));
                newKeys[idx + 1] = key;
                newChildren[idx + 1] = leftRight[0];
                newChildren[idx + 2] = leftRight[1];
                keys = newKeys;
                children = newChildren;
            }
        }

        public long findChild(K key, Comparator<K> comparator) {
            int pos = findPosition(key, comparator);
            if (pos == keys.length) {
                return children[pos];
            } else {
                K found = (K)keys[pos];
                int comp = comparator.compare(key, found);
                if (comp >= 0) {
                    return children[pos + 1];
                } else {
                    return children[pos];
                }
            }
        }

        @Override
        public Node<K> copyLeftSplit(int mid) {
            Branch<K> branch = new Branch<>();
            branch.keys = Arrays.copyOfRange(keys, 0, mid);
            branch.children = Arrays.copyOfRange(children, 0, mid + 1);
            return branch;
        }

        @Override
        public Node<K> copyRightSplit(int mid) {
            Branch<K> branch = new Branch<>();
            branch.keys = Arrays.copyOfRange(keys, mid, keys.length);
            branch.children = Arrays.copyOfRange(children, mid + 1, children.length);
            return branch;
        }
    }

    public final static class Leaf<K, V> extends Node<K> {
        protected Object[] values = new Object[0];

        public void put(K key, V value, Comparator<K> comparator) {
            // Find the position of the new key
            int idx = findPosition(key, comparator);
            // We have one in the middle of the array
            if (idx >= 0 && idx < keys.length) {
                K found = (K) keys[idx];
                if (comparator.compare(key, found) == 0) {
                    keys[idx] = key; // in theory we should just return,
                    values[idx] = value;
                } else {
                    // Increase the size of keys and children (children is always greater by 1 than keys)
                    Object[] newKeys = Arrays.copyOf(keys, keys.length + 1);
                    Object[] newValues = Arrays.copyOf(values, keys.length + 1);
                    // If the key is not the same as the one found, then it's inserted before the key
                    // as the index returned by the search will be the same or less than found.
                    // Copy the keys and children to the right
                    System.arraycopy(newKeys, idx, newKeys, idx + 1, keys.length);
                    System.arraycopy(newValues, idx, newValues, idx + 1, newValues.length);
                    // Set the new key
                    newKeys[idx] = key;
                    newValues[idx] = value;
                    keys = newKeys;
                    values = newValues;
                }
                return; // Finished insertion
            }
            // Increase the size of keys and children (children is always greater by 1 than keys)
            Object[] newKeys = Arrays.copyOf(keys, keys.length + 1);
            Object[] newValues = Arrays.copyOf(values, keys.length + 1);
            if (idx < 0) {
                // Shift right
                System.arraycopy(newKeys, 0, newKeys, 1, keys.length);
                System.arraycopy(newValues, 0, newValues, 1, keys.length);
                // Set key and value at 0
                newKeys[0] = key;
                newValues[0] = value;
                // Replace keys and values arrays
                keys = newKeys;
                values = newValues;
            } else if (idx >= keys.length) {
                // Push to the end
                newKeys[keys.length] = key;
                newValues[keys.length] = value;
                // Replace keys and values arrays
                keys = newKeys;
                values = newValues;
            }
        }

        @SuppressWarnings("unchecked")
        public boolean hasKey(K key, Comparator<K> comparator) {
            int idx = Arrays.binarySearch((K[])keys, key, comparator);
            return idx >= 0;
        }

        @SuppressWarnings("unchecked")
        public V getValue(K key, Comparator<K> comparator) {
            if (!hasKey(key, comparator)) {
                return null;
            }
            int idx = findPosition(key, comparator);
            return (V) values[idx];
        }

        @Override
        public Node<K> copyLeftSplit(int mid) {
            Leaf<K, V> leaf = new Leaf<>();
            leaf.keys = Arrays.copyOfRange(keys, 0, mid);
            leaf.values = Arrays.copyOfRange(values, 0, mid);
            return leaf;
        }

        @Override
        public Node<K> copyRightSplit(int mid) {
            Leaf<K, V> leaf = new Leaf<>();
            leaf.keys = Arrays.copyOfRange(keys, mid, keys.length);
            leaf.values = Arrays.copyOfRange(values, mid, values.length);
            return leaf;
        }
    }

    public static final class BTreeNodeSerializer implements Serializer {

        private final Serializer keySerializer, valueSerializer;

        public BTreeNodeSerializer(Serializer keySerializer, Serializer valueSerializer) {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        @Override
        public void serialize(DataOutput out, Object value) throws IOException {
            Node node = (Node) value;
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream os = new DataOutputStream(bytes);
            os.writeBoolean(Leaf.class.isAssignableFrom(node.getClass()));
            os.writeInt(node.keys.length);
            for (Object o : node.keys) {
                keySerializer.serialize(os, o);
            }
            if (Leaf.class.isAssignableFrom(node.getClass())) {
                for (Object o : ((Leaf) node).values) {
                    valueSerializer.serialize(os, o);
                }
            } else {
                for (long o : ((Branch) node).children) {
                    os.writeLong(o);
                }
            }
            out.writeUTF(getKey());
            out.write(bytes.toByteArray());
        }

        @Override
        public Object deserialize(DataInput in) throws IOException {
            String key = in.readUTF();
            if (!key.equals(getKey())) {
                throw new IOException("This is not of type: " + getKey());
            }
            boolean leaf = in.readBoolean();
            int keyLength = in.readInt();
            Object[] keys = new Object[keyLength];
            for (int i = 0; i < keyLength; i++) {
                keys[i] = keySerializer.deserialize(in);
            }

            // If we have a leaf node, then deserialize the values
            if (leaf) {
                Leaf node = new Leaf();
                node.keys = keys;
                node.values = new Object[keyLength];
                for (int i = 0; i < keyLength; i++) {
                    node.values[i] = valueSerializer.deserialize(in);
                }
                return node;
            }

            // Otherwise we should fetch the children
            Branch node = new Branch();
            node.keys = keys;
            node.children = new long[keyLength + 1];
            for (int i = 0; i < node.children.length; i++) {
                node.children[i] = in.readLong();
            }
            return node;
        }

        @Override
        public String getKey() {
            // Return the serializer key, which is a combination of this, key and value keys.
            return "BTMN:" + keySerializer.getKey() + ":" + valueSerializer.getKey();
        }
    }

    public static final class BTreeIterator<K, V> implements Iterator<Entry<K, V>> {

        private final BTreeMap2<K, V> map;
        private final K low, high;
        private final boolean lowInclusive, highInclusive;
        private final Comparator<K> comparator;
        private final AtomicInteger leafPos = new AtomicInteger(0);
        private Leaf<K, V> leaf;
        private Stack<Node<K>> stack = new Stack<>();
        private Stack<AtomicInteger> stackPos = new Stack<>();

        @SuppressWarnings("unchecked")
        public BTreeIterator(BTreeMap2<K, V> map, Comparator<K> comparator, K low, boolean lowInclusive,
                             K high, boolean highInclusive) {
            this.map = map;
            this.low = low;
            this.high = high;
            this.lowInclusive = lowInclusive;
            this.highInclusive = highInclusive;
            this.comparator = comparator;
            map.lock.readLock().lock();
            try {
                if (low == null) {
                    pointToStart();
                } else {
                    findLeaf(low);
                    int pos = leaf.findPosition(low, comparator);
                    K key = (K) leaf.keys[pos];
                    int comp = comparator.compare(low, key);
                    if (comp != 0) {
                        leafPos.set(pos);
                    } else {
                        leafPos.set(lowInclusive ? pos : pos + 1);
                    }
                }

                if (high != null && leaf != null) {
                    //check in bounds
                    int c = comparator.compare((K) leaf.keys[leafPos.get()], high);
                    if (c > 0 || (c == 0 && !highInclusive)) {
                        //out of high bound
                        leaf = null;
                        leafPos.set(-1);
                    }
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            } finally {
                map.lock.readLock().unlock();
            }
        }

        @SuppressWarnings("unchecked")
        private void pointToStart() throws IOException {
            Node<K> node = (Node<K>) map.store.get(map.rootPointer.get(), map.nodeSerializer);
            while (Branch.class.isAssignableFrom(node.getClass())) {
                stack.push(node);
                stackPos.push(new AtomicInteger(1)); // 1 because we want the next in the sequence.
                long child = ((Branch<K>) node).children[0]; // get the first child
                node = (Node<K>) map.store.get(child, map.nodeSerializer);
            }
            leaf = (Leaf<K, V>) node;
        }

        @SuppressWarnings("unchecked")
        private void findLeaf(K key) throws IOException {
            Node<K> node = (Node<K>) map.store.get(map.rootPointer.get(), map.nodeSerializer);
            while (Branch.class.isAssignableFrom(node.getClass())) {
                stack.push(node);
                int pos = ((Branch) node).findPosition(key, comparator);
                stackPos.push(new AtomicInteger(pos + 1));
                long child = ((Branch) node).children[pos];
                node = (Node<K>) map.store.get(child, map.nodeSerializer);
            }
            leaf = (Leaf<K, V>) node;
        }

        @SuppressWarnings("unchecked")
        private void advance() throws IOException {

            if (leaf != null && leafPos.get() < leaf.values.length) {
                return;
            }

            leaf = null;
            leafPos.set(-1);

            if (stack.isEmpty()) {
                return;
            }

            Branch<K> parent = (Branch<K>) stack.peek();
            int pos = stackPos.peek().getAndIncrement();
            if (pos < parent.children.length) {
                long t = parent.children[pos];
                Node<K> child = (Node<K>) map.store.get(t, map.nodeSerializer);
                if (Leaf.class.isAssignableFrom(child.getClass())) {
                    leaf = (Leaf<K, V>) child;
                    leafPos.set(0);
                } else {
                    stack.push(child);
                    stackPos.push(new AtomicInteger(0));
                    advance();
                    return;
                }
            } else {
                stack.pop();
                stackPos.pop();
                advance();
                return;
            }

            if (high != null && leaf != null) {
                int comp = comparator.compare((K) leaf.keys[leafPos.get()], high);
                if (comp > 0 || comp == 0 && !highInclusive) {
                    leaf = null;
                    leafPos.set(-1);
                }
            }
        }

        @Override
        public boolean hasNext() {
            try {
                map.lock.readLock().lock();
                if (leaf != null && leafPos.get() >= leaf.values.length) {
                    try {
                        advance();
                    } catch (IOException ex) {
                        throw new RuntimeException(ex); // FIXME: throw a runtime exception for now..
                    }
                } else if (leaf != null && high != null) {
                    int comp = comparator.compare((K) leaf.keys[leafPos.get()], high);
                    if (comp > 0 || (comp == 0 && !highInclusive)) {
                        leaf = null;
                        leafPos.set(-1);
                        stack.clear();
                        stackPos.clear();
                    }
                }
                return leaf != null;
            } finally {
                map.lock.readLock().unlock();
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public Entry<K, V> next() {
            try {
                map.lock.readLock().lock();

                // if we don't have a next value, then return null;
                if (!hasNext()) {
                    return null;
                }

                int pos = leafPos.getAndIncrement();

                K key = (K) leaf.keys[pos];
                V value = (V) leaf.values[pos];

                return new BaseMapEntry<>(key, value, map);

            } finally {
                map.lock.readLock().unlock();
            }
        }

    }

    public static final class ReverseBTreeIterator<K, V> implements Iterator<Entry<K, V>> {

        private final BTreeMap2<K, V> map;
        private final K low, high;
        private final boolean lowInclusive, highInclusive;
        private final Comparator<K> comparator;
        private final AtomicInteger leafPos = new AtomicInteger(0);
        private Leaf<K, V> leaf;
        private Stack<Node<K>> stack = new Stack<>();
        private Stack<AtomicInteger> stackPos = new Stack<>();

        @SuppressWarnings("unchecked")
        public ReverseBTreeIterator(BTreeMap2<K, V> map, Comparator<K> comparator, K low, boolean lowInclusive,
                             K high, boolean highInclusive) {
            this.map = map;
            this.low = low;
            this.high = high;
            this.lowInclusive = lowInclusive;
            this.highInclusive = highInclusive;
            this.comparator = comparator;
            map.lock.readLock().lock();
            try {
                if (low == null) {
                    pointToStart();
                } else {
                    findLeaf(low);
                    int pos = leaf.findPosition(low, comparator);
                    K key = (K) leaf.keys[pos];
                    int comp = comparator.compare(low, key);
                    if (comp != 0) {
                        leafPos.set(pos);
                    } else {
                        leafPos.set(lowInclusive ? pos : pos - 1);
                    }

                    if (leafPos.get() == -1) {
                        advance();
                    }
                }

                if (high != null && leaf != null) {
                    //check in bounds
                    int c = comparator.compare((K) leaf.keys[leafPos.get()], high);
                    if (c < 0 || (c == 0 && !highInclusive)) {
                        //out of high bound
                        leaf = null;
                        leafPos.set(-1);
                    }
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            } finally {
                map.lock.readLock().unlock();
            }
        }

        @SuppressWarnings("unchecked")
        private void pointToStart() throws IOException {
            Node<K> node = (Node<K>) map.store.get(map.rootPointer.get(), map.nodeSerializer);
            while (Branch.class.isAssignableFrom(node.getClass())) {
                stack.push(node);
                stackPos.push(new AtomicInteger(((Branch<K>)node).children.length - 2)); // 1 because we want the next in the sequence.
                long child = ((Branch<K>) node).children[((Branch<K>)node).children.length - 2]; // get the first child
                node = (Node<K>) map.store.get(child, map.nodeSerializer);
            }
            leaf = (Leaf<K, V>) node;
            leafPos.set(leaf.keys.length - 1);
        }

        @SuppressWarnings("unchecked")
        private void findLeaf(K key) throws IOException {
            Node<K> node = (Node<K>) map.store.get(map.rootPointer.get(), map.nodeSerializer);
            while (Branch.class.isAssignableFrom(node.getClass())) {
                stack.push(node);
                int pos = ((Branch) node).findPosition(key, comparator);
                stackPos.push(new AtomicInteger(pos - 1));
                long child = ((Branch) node).children[pos];
                node = (Node<K>) map.store.get(child, map.nodeSerializer);
            }
            leaf = (Leaf<K, V>) node;
        }

        @SuppressWarnings("unchecked")
        private void advance() throws IOException {

            if (leaf != null && leafPos.get() > -1) {
                return;
            }

            leaf = null;
            leafPos.set(-1);

            if (stack.isEmpty()) {
                return;
            }

            Branch<K> parent = (Branch<K>) stack.peek();
            int pos = stackPos.peek().getAndDecrement();
            if (pos >= 0) {
                long t = parent.children[pos];
                Node<K> child = (Node<K>) map.store.get(t, map.nodeSerializer);
                if (Leaf.class.isAssignableFrom(child.getClass())) {
                    leaf = (Leaf<K, V>) child;
                    leafPos.set(leaf.keys.length - 1);
                } else {
                    stack.push(child);
                    stackPos.push(new AtomicInteger(((Branch<K>)parent).children.length - 1));
                    advance();
                    return;
                }
            } else {
                stack.pop();
                stackPos.pop();
                advance();
                return;
            }

            if (high != null && leaf != null) {
                int comp = comparator.compare((K) leaf.keys[leafPos.get()], high);
                if (comp < 0 || comp == 0 && !highInclusive) {
                    leaf = null;
                    leafPos.set(-1);
                }
            }
        }

        @Override
        public boolean hasNext() {
            try {
                map.lock.readLock().lock();
                if (leaf != null && leafPos.get() >= leaf.values.length) {
                    try {
                        advance();
                    } catch (IOException ex) {
                        throw new RuntimeException(ex); // FIXME: throw a runtime exception for now..
                    }
                } else if (leaf != null && high != null) {
                    int comp = comparator.compare((K) leaf.keys[leafPos.get()], high);
                    if (comp < 0 || (comp == 0 && !highInclusive)) {
                        leaf = null;
                        leafPos.set(-1);
                        stack.clear();
                        stackPos.clear();
                    }
                }
                return leaf != null;
            } finally {
                map.lock.readLock().unlock();
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public Entry<K, V> next() {
            try {
                map.lock.readLock().lock();

                // if we don't have a next value, then return null;
                if (!hasNext()) {
                    return null;
                }

                int pos = leafPos.getAndDecrement();

                K key = (K) leaf.keys[pos];
                V value = (V) leaf.values[pos];

                return new BaseMapEntry<>(key, value, map);

            } finally {
                map.lock.readLock().unlock();
            }
        }

    }
}
