package com.pungwe.db.engine.collections;

import com.pungwe.db.engine.io.exceptions.DuplicateKeyException;
import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.engine.io.store.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/* FIXME: This needs to be rethought.
 * It's slow when the key size is bigger than 10 and we lose size. when it's persistend to disk */
/**
 *
 * Created by ian on 25/05/2016.
 */
public class BTreeMap<K, V> extends BaseMap<K, V> {

    private static final Logger log = LoggerFactory.getLogger(BTreeMap.class);

    private final Store store;
    private final Serializer keySerializer;
    private final Serializer valueSerializer;
    private final BTreeNodeSerializer nodeSerializer = new BTreeNodeSerializer();
    private final long maxNodeSize;
    private volatile long rootPointer;
    protected AtomicLong size;

    public BTreeMap(Store store, Comparator<K> keyComparator, long maxNodeSize) throws IOException {
        this(store, keyComparator, new ObjectSerializer(), new ObjectSerializer(), maxNodeSize, -1);
    }

    public BTreeMap(Store store, Comparator<K> keyComparator,
                    Serializer keySerializer, Serializer valueSerializer, long maxNodeSize) throws IOException {
        this(store, keyComparator, keySerializer, valueSerializer, maxNodeSize, -1l);
    }

    public BTreeMap(Store store, Comparator<K> keyComparator,
                    Serializer keySerializer, Serializer valueSerializer, long maxNodeSize, long rootPointer)
            throws IOException {
        super(keyComparator);
        this.store = store;
        this.maxNodeSize = maxNodeSize;
        this.rootPointer = rootPointer;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        if (rootPointer == -1) {
            LeafNode<K, V> root = new LeafNode<K, V>(keyComparator);
            this.rootPointer = store.add(root, nodeSerializer);
        }
    }

    public long getPointer() {
        return rootPointer;
    }

    @Override
    public Entry<K, V> getEntry(K key) {
        lock.readLock().lock();
        try {
            LeafNode<K, Object> leaf = (LeafNode<K, Object>) findLeaf((K) key);
            if (leaf != null && leaf.hasKey(key)) {
                Object value = leaf.getValue(key);
                return new BaseMapEntry<>(key, (V) value, this);
            } else {
                return null;
            }
        } catch (IOException ex) {
            log.error("Could not find value for key: " + key, ex);
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    private LeafNode<K, V> findLeaf(K key) throws IOException {
        long current = rootPointer;
        BTreeNode<K, V> node = (BTreeNode<K, V>) store.get(current, nodeSerializer);
        while (!(node instanceof LeafNode)) {
            current = ((BranchNode<K>) node).getChild((K) key);
            node = (BTreeNode<K, V>) store.get(current, nodeSerializer);
        }
        return (LeafNode<K, V>) node;
    }

    @Override
    public Entry<K, V> putEntry(final K key, final V value, boolean replace) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }
        try {
            lock.writeLock().lock();

            // Set current to root record
            long current = rootPointer;
            long[] pointers = new long[1];
            BTreeNode<K, V>[] parentNodes = new BTreeNode[1];

            int pos = 0;
            BTreeNode<K, V> node = (BTreeNode<K, V>) store.get(current, nodeSerializer);
            parentNodes[0] = node;
            pointers[pos] = current;
            pos++;
            while (!(node instanceof LeafNode)) {
                current = ((BranchNode<K>) node).getChild(key);
                node = (BTreeNode<K, V>) store.get(current, nodeSerializer);

                // Make sure we have space
                if (pos == pointers.length) {
                    pointers = Arrays.copyOf(pointers, pointers.length + 1);
                    parentNodes = Arrays.copyOf(parentNodes, parentNodes.length + 1);
                }

                // Add the node to the stack
                pointers[pos] = current;
                parentNodes[pos++] = node;

            }

            // Last item is the leaf node
            LeafNode<K, Object> leaf = (LeafNode<K, Object>) node;

            // We need to check if the value exists
            boolean exists = leaf.hasKey(key);
            leaf.putValue(key, value, replace);
            // Node is not safe and must be split
            if (leaf != null && maxNodeSize > -1 && leaf.keys.length > maxNodeSize) {
                split(Arrays.copyOf(parentNodes, parentNodes.length), pointers);
            } else {
                saveNodes(parentNodes, pointers);
            }

            if (!exists) {
                incrementSize();
            }

        } catch (IOException ex) {
            log.error("Could not add value for key: " + key, ex);
            return null;
        } finally {
            if (lock.writeLock().isHeldByCurrentThread()) {
                lock.writeLock().unlock();
            }
        }
        return new BaseMapEntry<K, V>(key, value, this);
    }

    private void saveNodes(BTreeNode<K, V>[] nodes, long[] pointers) throws IOException {
        // Reverse iterate through each node...
        for (int i = nodes.length - 1; i > -1; i--) {
            long current = pointers[i];
            BTreeNode<K, V> node = nodes[i];
            BTreeNode<K, V> parent = i - 1 < 0 ? null : nodes[i - 1];
            long newPointer = store.update(current, node, nodeSerializer);
            if (current == rootPointer && newPointer != current) {
                rootPointer = newPointer;
            }
            if (parent != null && newPointer != current) {
                // FIXME: Update all find methods to use Arrays.binarySearch
                long[] children = ((BranchNode<K>)parent).children;
                int index = Arrays.binarySearch(children, current);
                if (index > 0 && index < children.length) {
                    children[index] = newPointer;
                }
            }
        }
    }

    private void split(BTreeNode<K, V>[] parents, long[] pointers) throws IOException {
        long offset = pointers[pointers.length - 1];
        BTreeNode<K, V> node = parents[pointers.length - 1];
        int mid = (node.keys.length - 1) >>> 1;
        K key = node.getKey(mid);
        BTreeNode<K, ?> left = node.copyLeftSplit(mid);
        BTreeNode<K, ?> right = node.copyRightSplit(mid);
        long[] children = new long[2];
        children[0] = store.update(offset, left, nodeSerializer); // left replaces the old left...
        children[1] = store.add(right, nodeSerializer);

        // If we are already the root node, we create a new one...
        if (pointers.length == 1) {
            BranchNode<K> newRoot = new BranchNode<K>((Comparator<K>) comparator());
            newRoot.putChild(key, children);
            rootPointer = store.add(newRoot, nodeSerializer);
            return;
        }

        // Otherwise we find the parent.
        BranchNode<K> parent = (BranchNode<K>)parents[pointers.length - 2];
        parent.putChild(key, children);

        if (parent.keys.length > maxNodeSize) {
            split(Arrays.copyOf(parents, parents.length - 1), Arrays.copyOf(pointers, pointers.length - 1));
            return;
        } else {
            saveNodes(Arrays.copyOf(parents, parents.length - 1), Arrays.copyOf(pointers, pointers.length - 1));
        }
    }

    @Override
    protected Iterator<Entry<K, V>> iterator(Comparator<? super K> comparator, K low, boolean lowInclusive, K high,
                                             boolean highInclusive) {
        return new BTreeNodeIterator<K, V>(this, (Comparator<K>) comparator, low, lowInclusive, high, highInclusive);
    }

    @Override
    protected Iterator<Entry<K, V>> descendingIterator(Comparator<? super K> comparator, K low, boolean lowInclusive,
                                                       K high, boolean highInclusive) {
        return new ReverseBTreeNodeIterator<K, V>(this, (Comparator<K>) comparator, low, lowInclusive, high, highInclusive);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return false;
    }

    @Override
    public int size() {
        return (int) Math.min(sizeLong(), Integer.MAX_VALUE);
    }

    public long sizeLong() {
        if (size != null) {
            return size.get();
        }
        return 0l;
    }

    private void incrementSize() {
        if (size == null) {
            size = new AtomicLong();
        }
        size.getAndIncrement();
    }

    @Override
    public boolean isEmpty() {
        return sizeLong() == 0;
    }

    @Override
    public void clear() {

    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new EntrySet<>(this);
    }

    static final class BTreeNodeIterator<K, V> implements Iterator<Map.Entry<K, V>> {

        private static final Logger log = LoggerFactory.getLogger(BTreeNodeIterator.class);

        final BTreeMap<K, V> map;
        final Comparator<K> comparator;
        private Stack<BranchNode<K>> stack = new Stack<>();
        private Stack<AtomicInteger> stackPos = new Stack<>();
        private LeafNode<K, ?> leaf;
        private int leafPos = 0;
        private final Object high;
        private final boolean highInclusive;

        public BTreeNodeIterator(BTreeMap<K, V> map, Comparator<K> comparator, Object low, boolean lowInclusive,
                                 Object high, boolean highInclusive) {
            this.map = map;
            this.comparator = comparator;
            this.high = high;
            this.highInclusive = highInclusive;
            try {
                this.map.lock.readLock().lock();
                if (low == null) {
                    pointToStart();
                } else {
                    // Find the starting point
                    findLeaf((K) low);
                    int pos = leaf.findPosition((K) low);
                    K k = leaf.getKey(pos);
                    int comp = comparator.compare((K) low, k);
                    if (comp > 0) {
                        leafPos = pos;
                    } else if (comp == 0) {
                        leafPos = lowInclusive ? pos : pos + 1;
                    } else if (comp < 0) {
                        leafPos = pos;
                    }
                }

                if (high != null && leaf != null) {
                    //check in bounds
                    int c = comparator.compare(leaf.getKey(leafPos), (K) high);
                    if (c > 0 || (c == 0 && !highInclusive)) {
                        //out of high bound
                        leaf = null;
                        leafPos = -1;
                        //$DELAY$
                    }
                }
            } catch (IOException ex) {
                log.error("Could not find start of btree");
                throw new RuntimeException(ex);
            } finally {
                map.lock.readLock().unlock();
            }
        }

        private void findLeaf(K key) throws IOException {
            long current = map.rootPointer;
            BTreeNode<K, ?> node = (BTreeNode<K, V>) map.store.get(current, map.nodeSerializer);
            while (!(node instanceof LeafNode)) {
                stack.push((BranchNode<K>) node);
                int pos = ((BranchNode<K>) node).findChildPosition((K) key);
                stackPos.push(new AtomicInteger(pos + 1));
                current = ((BranchNode<K>) node).children[pos];
                node = (BTreeNode<K, V>) map.store.get(current, map.nodeSerializer);
            }
            leaf = (LeafNode<K, ?>) node;
        }

        private void pointToStart() throws IOException {
            BTreeNode<K, ?> node = (BTreeNode<K, V>) map.store.get(map.rootPointer, map.nodeSerializer);
            while (!(node instanceof LeafNode)) {
                stack.push((BranchNode<K>) node);
                stackPos.push(new AtomicInteger(1));
                long child = ((BranchNode<K>) node).children[0];
                node = (BTreeNode<K, V>) map.store.get(child, map.nodeSerializer);
            }
            leaf = (LeafNode<K, ?>) node;
        }

        private void advance() throws IOException {

            if (leaf != null && leafPos < leaf.values.length) {
                return; // nothing to see here
            }

            leaf = null;
            leafPos = -1; // reset to 0

            if (stack.isEmpty()) {
                return; // nothing to see here
            }

            BranchNode<K> parent = stack.peek(); // get the immediate parent

            int pos = stackPos.peek().getAndIncrement(); // get the immediate parent position.
            if (pos < parent.children.length) {
                long t = parent.children[pos];
                BTreeNode<K, ?> child = (BTreeNode<K, V>) map.store.get(t, map.nodeSerializer);
                if (child instanceof LeafNode) {
                    leaf = (LeafNode<K, V>) child;
                    leafPos = 0;
                } else {
                    stack.push((BranchNode<K>) child);
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

            if (high != null && leaf != null) {
                int comp = comparator.compare(leaf.getKey(leafPos), (K) high);
                if (comp > 0 || (comp == 0 && !highInclusive)) {
                    leaf = null;
                    leafPos = -1;
                }
            }
        }

        @Override
        public boolean hasNext() {
            try {
                map.lock.readLock().lock();
                if (leaf != null && leafPos >= leaf.values.length) {
                    try {
                        advance();
                    } catch (IOException ex) {
                        throw new RuntimeException(ex); // FIXME: throw a runtime exception for now..
                    }
                } else if (leaf != null && high != null) {
                    int comp = comparator.compare(leaf.getKey(leafPos), (K) high);
                    if (comp > 0 || (comp == 0 && !highInclusive)) {
                        leaf = null;
                        leafPos = -1;
                        stack.clear();
                        stackPos.clear();
                    }
                }
                return leaf != null;
            } finally {
                map.lock.readLock().unlock();
            }
        }

        @Override
        public Map.Entry<K, V> next() {

            try {
                map.lock.readLock().lock();

                // if we don't have a next value, then return null;
                if (!hasNext()) {
                    return null;
                }

                int pos = leafPos++;

                Object key = leaf.keys[pos];
                Object value = leaf.values[pos];

                return new BaseMapEntry<>((K) key, (V) value, map);

            } finally {
                map.lock.readLock().unlock();
            }
        }
    }

    /**
     * Created by 917903 on 04/03/2015.
     */
    final static class ReverseBTreeNodeIterator<K, V> implements Iterator<Map.Entry<K, V>> {

        final static Logger log = LoggerFactory.getLogger(ReverseBTreeNodeIterator.class);

        final BTreeMap<K, V> map;
        private Stack<BranchNode<K>> stack = new Stack<>();
        private Stack<AtomicInteger> stackPos = new Stack<>();
        private LeafNode<K, ?> leaf;
        private int leafPos = 0;
        private final Object hi;
        private final boolean hiInclusive;
        private final Comparator<K> comparator;

        public ReverseBTreeNodeIterator(BTreeMap<K, V> map, Comparator<K> comparator) {
            this.map = map;
            hi = null;
            hiInclusive = false;
            this.comparator = comparator;
            try {
                pointToStart();
            } catch (IOException ex) {
                log.error("Could not find start of btree", ex);
                throw new RuntimeException(ex);
            }
        }

        public ReverseBTreeNodeIterator(BTreeMap<K, V> map, Comparator<K> comparator, Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
            this.map = map;
            this.hi = hi;
            this.hiInclusive = hiInclusive;
            this.comparator = comparator;
            try {

                if (lo == null) {
                    pointToStart();
                } else {
                    // Find the starting point
                    findLeaf((K)lo);
                    int pos = leaf.findPosition((K)lo);
                    K k = leaf.getKey(pos);
                    int comp = comparator.compare((K) lo, k);
                    if (comp < 0) {
                        leafPos = pos;
                    } else if (comp == 0) {
                        leafPos = loInclusive ? pos : pos - 1;
                    } else if (comp > 0) {
                        leafPos = pos;
                    }

                    if (leafPos == -1) {
                        advance();
                    }
                }

                if (hi != null && leaf != null) {
                    //check in bounds
                    //int c = leaf.compare(m.keySerializer, currentPos, hi);
                    int c = comparator.compare(leaf.getKey(leafPos), (K) hi);
                    if (c < 0 || (c == 0 && !hiInclusive)) {
                        //out of high bound
                        leaf = null;
                        leafPos = -1;
                    }
                }
            } catch (IOException ex) {
                log.error("Could not find start of btree");
                throw new RuntimeException(ex);
            }
        }

        private void findLeaf(K key) throws IOException {
            long current = map.rootPointer;
            BTreeNode<K, ?> node = (BTreeNode<K, ?>)map.store.get(current, map.nodeSerializer);
            while (!(node instanceof LeafNode)) {
                stack.push((BranchNode<K>) node);
                int pos = ((BranchNode<K>) node).findChildPosition((K) key);
                stackPos.push(new AtomicInteger(pos - 1));
                current = ((BranchNode<K>) node).children[pos];
                node = (BTreeNode<K, ?>)map.store.get(current, map.nodeSerializer);
            }
            leaf = (LeafNode<K, ?>) node;
        }

        private void pointToStart() throws IOException {
            try {
                //map.lock.readLock().lock();
                BTreeNode<K, ?> node = (BTreeNode<K, ?>)map.store.get(map.rootPointer, map.nodeSerializer);
                while (!(node instanceof LeafNode)) {
                    stack.push((BranchNode<K>) node);
                    stackPos.push(new AtomicInteger(((BranchNode<K>) node).children.length - 2));
                    long child = ((BranchNode<K>) node).children[((BranchNode<K>) node).children.length - 1];
                    node = (BTreeNode<K, ?>)map.store.get(child, map.nodeSerializer);
                }
                leaf = (LeafNode<K, ?>) node;
                leafPos = leaf.keys.length - 1;
            } finally {
                //map.lock.readLock().unlock();
            }
        }

        private void advance() throws IOException {
            try {
                map.lock.readLock().lock();

                if (leaf != null && leafPos > 0) {
                    return; // nothing to see here
                }

                leaf = null;
                leafPos = -1; // reset to 0

                if (stack.isEmpty()) {
                    return; // nothing to see here
                }

                BranchNode<K> parent = stack.peek(); // get the immediate parent

                int pos = stackPos.peek().getAndDecrement(); // get the immediate parent position.
                if (pos >= 0) {
                    long t = parent.children[pos];
                    BTreeNode<K, ?> child = (BTreeNode<K, ?>)map.store.get(t, map.nodeSerializer);
                    if (child instanceof LeafNode) {
                        leaf = (LeafNode<K, V>) child;
                        leafPos = leaf.keys.length - 1;
                    } else {
                        stack.push((BranchNode<K>) child);
                        stackPos.push(new AtomicInteger(((BranchNode<K>) child).children.length - 1));
                        advance();
                        return;
                    }
                } else {
                    stack.pop(); // remove last node
                    stackPos.pop();
                    advance();
                    return;
                }

                if (hi != null && leaf != null) {
                    int comp = comparator.compare(leaf.getKey(leafPos), (K) hi);
                    if (comp < 0 || (comp == 0 && !hiInclusive)) {
                        leaf = null;
                        leafPos = -1;
                    }
                }
            } finally {
                map.lock.readLock().unlock();
            }
        }

        @Override
        public boolean hasNext() {
            if (leaf != null && leafPos < 0) {
                try {
                    advance();
                } catch (IOException ex) {
                    throw new RuntimeException(ex); // FIXME: throw a runtime exception for now..
                }
            } else if (hi != null && leaf != null) {
                int comp = comparator.compare(leaf.getKey(leafPos), (K) hi);
                if (comp < 0 || (comp == 0 && !hiInclusive)) {
                    leaf = null;
                    leafPos = -1;
                }
            }
            return leaf != null;
        }

        @Override
        public Map.Entry<K, V> next() {

            try {
                //map.lock.readLock().lock();

                // if we don't have a next value, then return null;
                if (!hasNext()) {
                    return null;
                }

                int pos = leafPos--;

                Object key = leaf.keys[pos];
                Object value = leaf.values[pos];

                return new BaseMapEntry<K, V>((K)key, (V)value, map);

            } finally {
                //map.lock.readLock().unlock();
            }
        }
    }

    static abstract class BTreeNode<K, V> {
        final Comparator<K> comparator;

        public BTreeNode(Comparator<K> comparator) {
            this.comparator = comparator;
        }

        protected Object[] keys = new Object[0];

        public abstract BTreeNode<K, V> copyRightSplit(int mid);

        public abstract BTreeNode<K, V> copyLeftSplit(int mid);

        protected void addKey(int pos, K key) {
            Object[] newKeys = Arrays.copyOf(keys, keys.length + 1);
            if (pos < keys.length) {
                System.arraycopy(newKeys, pos, newKeys, (pos + 1), keys.length - pos);
            }
            newKeys[pos] = key;
            keys = newKeys;
        }

        protected void setKey(int pos, K key) {
            assert pos < keys.length : "Cannot overwrite a key that doesn't exist";
            keys[pos] = key;
        }

        protected K getKey(int pos) {
            return (K) keys[pos];
        }

        protected void removeKey(int pos) {
            Object[] newKeys = new Object[keys.length - 1];
            System.arraycopy(keys, 0, newKeys, 0, pos);
            if (pos < newKeys.length) {
                System.arraycopy(keys, pos + 1, newKeys, pos, (newKeys.length - pos));
            }
            keys = newKeys;
        }

        protected int findPosition(K key) {
            int low = 0;
            int high = keys.length - 1;
            return findPosition(key, low, high);
        }

        protected int findPosition(K key, int low, int high) {

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

            return findPosition(key, low, high);
        }
    }

    final static class BranchNode<K> extends BTreeNode<K, Long> {
        // FIXME: Make this a counting btree..
        //protected final AtomicLong size = new AtomicLong();

        protected long[] children;

        public BranchNode(Comparator<K> comparator) {
            this(comparator, new long[0]);
        }

        public BranchNode(Comparator<K> comparator, long[] children) {
            super(comparator);
            this.children = children;
        }

        public void putChild(K key, long[] child) throws DuplicateKeyException {

            int pos = findPosition(key);

            assert child.length == 2;
            long left = child[0];
            long right = child[1];

            if (keys.length == 0) {
                addKey(0, key);
                addChild(0, left);
                addChild(1, right);
                return;
            }

            // Add something to the end
            if (pos == keys.length) {
                addKey(pos, key);
                setChild(pos, left);
                addChild(pos + 1, right);
                return;
            }

            // Check the keys
            K existing = getKey(pos);
            int comp = comparator.compare(key, existing);

            if (pos == 0) {
                if (comp == 0) {
                    throw new DuplicateKeyException("Key already exists: " + key);
                } else if (comp > 0) {
                    addKey(1, key);
                    setChild(1, left);
                    addChild(2, right);
                } else {
                    addKey(0, key);
                    setChild(0, left);
                    addChild(1, right);
                }
                return;
            }

            if (comp == 0) {
                throw new DuplicateKeyException("Key already exists: " + key);
            } else if (comp < 0) {
                addKey(pos, key);
                setChild(pos, left);
                addChild(pos + 1, right);
                // FIXME: We shouldn't get the below. Let's see how code coverage comes out...
            } else if (comp > 0) {
                addKey(pos + 1, key);
                setChild(pos + 1, left);
                addChild(pos + 2, right);
            }
        }

        public long getChild(K key) {
            int pos = findPosition(key);
            if (pos == keys.length) {
                return children[children.length - 1];
            } else {
                K found = getKey(pos);
                int comp = comparator.compare(key, found);
                if (comp >= 0) {
                    return children[pos + 1];
                } else {
                    return children[pos]; // left
                }
            }
        }

        public void addChild(int pos, long child) {
            long[] newChildren = Arrays.copyOf(children, children.length + 1);
            if (pos < children.length) {
                int newPos = pos + 1;
                System.arraycopy(children, pos, newChildren, newPos, children.length - pos);
            }
            newChildren[pos] = child;
            children = newChildren;
        }

        public void setChild(int pos, long child) {
            assert pos < children.length;
            children[pos] = child;
        }

        @Override
        public BTreeNode<K, Long> copyRightSplit(int mid) {
            // Create a right hand node
            BranchNode<K> right = new BranchNode<>(comparator);
            right.keys = Arrays.copyOfRange(keys, mid + 1, keys.length);
            right.children = Arrays.copyOfRange(children, mid + 1, children.length);
            assert right.keys.length < right.children.length : "Keys and Children are equal";
            return right;
        }

        @Override
        public BTreeNode<K, Long> copyLeftSplit(int mid) {
            // Create a right hand node
            BranchNode<K> left = new BranchNode<>(comparator);
            left.keys = Arrays.copyOfRange(keys, 0, mid);
            left.children = Arrays.copyOfRange(children, 0, mid + 1);
            return left;
        }

        public int findChildPosition(K key) {
            int pos = findPosition(key);
            if (pos == keys.length) {
                return pos;
            } else {
                K found = getKey(pos);
                int comp = comparator.compare(key, found);
                if (comp >= 0) {
                    return pos + 1;
                } else {
                    return pos;
                }
            }
        }
    }

    final static class LeafNode<K, V> extends BTreeNode<K, V> {
        protected Object[] values;

        public LeafNode(Comparator<K> comparator) {
            this(comparator, new Object[0]);
        }

        public LeafNode(Comparator<K> comparator, Object[] values) {
            super(comparator);
            this.values = values;
        }

        private void addValue(int pos, V value) {
            Object[] newValues = Arrays.copyOf(values, values.length + 1);
            if (pos < values.length) {
                System.arraycopy(values, pos, newValues, pos + 1, values.length - pos);
            }
            newValues[pos] = value;
            values = newValues;
        }

        protected void setValue(int pos, V value) {
            assert pos < values.length : "Cannot overwrite a key that doesn't exist";
            values[pos] = value;
        }

        protected V removeValue(int pos) {
            Object[] newValues = new Object[values.length - 1];
            System.arraycopy(values, 0, newValues, 0, pos);
            if (pos < newValues.length) {
                System.arraycopy(values, pos + 1, newValues, pos, (newValues.length - pos));
            }
            values = newValues;

            return (V) values[pos];
        }

        protected K getKey(int pos) {
            return (K) keys[pos];
        }

        public V putValue(K key, V value, boolean replace) throws DuplicateKeyException {
            // Find out where the key should be
            int pos = findPosition(key);

            // New value...
            if (pos == keys.length) {
                addKey(pos, key);
                addValue(pos, value);
                return value;
            }

            // Get the key
            K existing = getKey(pos);

            // Compare the new key to the existing key
            int comp = comparator.compare(key, existing);

            // Compare the two keys
            if (comp == 0 && replace) {
                setKey(comp, key);
                setValue(comp, value);
            } else if (comp == 0 && !replace) {
                throw new DuplicateKeyException("Duplicate key found: " + key);
            } else if (comp > 0) {
                addKey(pos + 1, key);
                addValue(pos + 1, value);
            } else {
                addKey(pos, key);
                addValue(pos, value);
            }

            return value;
        }

        public V remove(K key) {
            int pos = findPosition(key);

            if (pos < keys.length) {
                K existing = getKey(pos);
                int comp = comparator.compare(key, existing);
                if (comp == 0) {
                    removeKey(pos);
                    return removeValue(pos);
                }
            }

            return null;
        }

        public V getValue(K key) {
            int pos = findPosition(key);
            // Key does not exist
            if (pos == keys.length) {
                return null;
            }

            // Check the key against the one found
            K existing = getKey(pos);

            int comp = comparator.compare(key, existing);

            // If it's the same, then return the value, if it's not, then throw an error
            return comp == 0 ? (V) values[pos] : null;
        }

        @Override
        public BTreeNode<K, V> copyRightSplit(int mid) {
            // Create a right hand node
            LeafNode<K, V> right = new LeafNode<>(comparator);
            right.keys = Arrays.copyOfRange(keys, mid, keys.length);
            right.values = Arrays.copyOfRange(values, mid, values.length);
            return right;
        }

        @Override
        public BTreeNode<K, V> copyLeftSplit(int mid) {
            // Create a right hand node
            LeafNode<K, V> left = new LeafNode<>(comparator);
            left.keys = Arrays.copyOfRange(keys, 0, mid);
            left.values = Arrays.copyOfRange(values, 0, mid);
            return left;
        }

        public boolean hasKey(K key) {
            int pos = findPosition(key);
            if (pos == keys.length) {
                return false;
            }
            K found = (K) keys[pos];
            return comparator.compare(key, found) == 0;
        }
    }

    // FIXME: Put a serializer Identifier in.
    private final class BTreeNodeSerializer implements Serializer {
        @Override
        public void serialize(DataOutput out, Object value) throws IOException {
            BTreeNode node = (BTreeNode) value;
            out.writeBoolean(value instanceof LeafNode);
            out.writeInt(node.keys.length);
            for (Object k : node.keys) {
                keySerializer.serialize(out, k);
            }
            if (value instanceof LeafNode) {
                for (Object o : ((LeafNode) value).values) {
                    valueSerializer.serialize(out, o);
                }
            } else {
                for (long o : ((BranchNode) value).children) {
                    assert o > 0 : " pointer is 0";
                    out.writeLong(o);
                }
            }
            return;
        }

        @Override
        public Object deserialize(DataInput in) throws IOException {
            boolean leaf = in.readBoolean();
            int keyLength = in.readInt();
            BTreeNode<K, ?> node = leaf ? new LeafNode<K, V>((Comparator<K>) comparator()) :
                    new BranchNode<K>((Comparator<K>) comparator());
            node.keys = new Object[keyLength];
            for (int i = 0; i < keyLength; i++) {
                node.keys[i] = keySerializer.deserialize(in);
            }
            if (leaf) {
                ((LeafNode<K, V>) node).values = new Object[keyLength];
                for (int i = 0; i < keyLength; i++) {
                    ((LeafNode<K, V>) node).values[i] = valueSerializer.deserialize(in);
                }
            } else {
                ((BranchNode<K>) node).children = new long[keyLength + 1];
                for (int i = 0; i < ((BranchNode<K>) node).children.length; i++) {
                    long offset = in.readLong();
                    assert offset > 0 : "Id is 0...";
                    ((BranchNode<K>) node).children[i] = offset;
                }
            }
            return node;
        }

        @Override
        public String getKey() {
            return "BT";
        }
    }
}
