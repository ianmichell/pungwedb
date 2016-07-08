package com.pungwe.db.engine.collections;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ian on 07/07/2016.
 */
public abstract class AbstractBTreeMap<K,V> implements ConcurrentNavigableMap<K,V> {

    // FIXME: Do we need levels?
    private final AtomicInteger levels = new AtomicInteger();
    protected final Comparator<K> comparator;

    public AbstractBTreeMap(Comparator<K> comparator) {
        this.comparator = comparator;
    }

    @Override
    public ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
        return subMap(null, false, toKey, inclusive);
    }

    @Override
    public ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
        return subMap(fromKey, inclusive, null, false);
    }

    @Override
    public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
        return subMap(fromKey, true, toKey, false);
    }

    @Override
    public ConcurrentNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return new SubMap<>(this, comparator, fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public ConcurrentNavigableMap<K, V> headMap(K toKey) {
        return subMap(null, false, toKey, true);
    }

    @Override
    public ConcurrentNavigableMap<K, V> tailMap(K fromKey) {
        return subMap(fromKey, true, null, false);
    }

    @Override
    public ConcurrentNavigableMap<K, V> descendingMap() {
        return new DescendingMap<>(this, (o1, o2) -> -comparator.compare(o1, o2), null, false, null, false);
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        return new KeySet<K>(this);
    }

    @Override
    public NavigableSet<K> keySet() {
        return new KeySet<K>(this);
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
        return descendingMap().keySet();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new EntrySet<>(this);
    }

    @Override
    public Entry<K, V> lowerEntry(K key) {
        Iterator<Entry<K, V>> it = reverseIterator();
        return it.hasNext() ? it.next() : null;
    }

    @Override
    public K lowerKey(K key) {
        Entry<K, V> entry = lowerEntry(key);
        if (entry == null) {
            return null;
        }
        return entry.getKey();
    }

    @Override
    public Entry<K, V> floorEntry(K key) {
        Iterator<Entry<K, V>> it = reverseIterator(null, true, key, true);
        Entry<K, V> same = null;
        Entry<K, V> next = null;
        if (it.hasNext()) {
            same = it.next();
        }
        if (it.hasNext()) {
            next = it.next();
        }
        if (next == null && same != null && comparator().compare(key, same.getKey()) >= 0) {
            return same;
        } else if (next != null && comparator().compare(key, next.getKey()) > 0) {
            return next;
        }
        return null;
    }

    @Override
    public K floorKey(K key) {
        Entry<K, V> entry = floorEntry(key);
        if (entry == null) {
            return null;
        }
        return entry.getKey();
    }

    @Override
    public Entry<K, V> ceilingEntry(K key) {
        Iterator<Entry<K, V>> it = iterator(key, true, null, true);
        Entry<K, V> same = null;
        Entry<K, V> next = null;
        if (it.hasNext()) {
            same = it.next();
        }
        if (it.hasNext()) {
            next = it.next();
        }
        if (next == null && same != null && comparator().compare(key, same.getKey()) <= 0) {
            return same;
        } else if (next != null && comparator().compare(key, next.getKey()) < 0) {
            return next;
        }
        return null;
    }

    @Override
    public K ceilingKey(K key) {
        return null;
    }

    @Override
    public Entry<K, V> higherEntry(K key) {
        Iterator<Entry<K, V>> it = iterator(key, false, null, true);
        return it.hasNext() ? it.next() : null;
    }

    @Override
    public K higherKey(K key) {
        Entry<K, V> entry = higherEntry(key);
        if (entry == null) {
            return null;
        }
        return entry.getKey();
    }

    @Override
    public Entry<K, V> pollFirstEntry() {
        Entry<K, V> entry = firstEntry();
        if (entry != null) {
            removeEntry(entry.getKey());
        }
        return entry;
    }

    @Override
    public Entry<K, V> pollLastEntry() {
        Entry<K, V> entry = lastEntry();
        if (entry != null) {
            removeEntry(entry.getKey());
        }
        return entry;
    }

    @Override
    public Entry<K, V> firstEntry() {
        // Stream the entry set to get the first entry
        return entrySet().stream().filter(e -> !((BTreeEntry<K,V>)e).isDeleted()).findFirst().orElse(null);
    }

    @Override
    public Entry<K, V> lastEntry() {
        return descendingMap().firstEntry();
    }

    @Override
    public Comparator<? super K> comparator() {
        return comparator;
    }

    @Override
    public K firstKey() {
        Entry<K,V> firstEntry = firstEntry();
        if (firstEntry != null) {
            return firstEntry.getKey();
        }
        return null;
    }

    @Override
    public K lastKey() {
        Entry<K,V> lastEntry = lastEntry();
        if (lastEntry != null) {
            return lastEntry.getKey();
        }
        return null;
    }

    @Override
    public V putIfAbsent(K key, V value) {
        if (containsKey(key)) {
            return null;
        }
        return put(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return remove(key) != null;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return put(key, newValue) != null;
    }

    @Override
    public V replace(K key, V value) {
        return put(key, value);
    }

    @Override
    public int size() {
        return (int)Math.min(sizeLong(), Integer.MAX_VALUE);
    }

    @Override
    public boolean isEmpty() {
        return size() > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean containsKey(Object key) {
        return get((K)key) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        return values().parallelStream().anyMatch(v -> v != null && v.equals(value));
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        BTreeEntry<K, V> entry = getEntry((K)key);
        if (entry == null || entry.isDeleted()) {
            return null;
        }
        return entry.getValue();
    }

    @Override
    public V put(K key, V value) {
        Entry<K,V> entry = putEntry(new BTreeEntry<K, V>(key, value, false));
        return entry.getValue();
    }

    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        return removeEntry((K)key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        m.entrySet().parallelStream().forEach(this::putEntry);
    }

    @Override
    public Collection<V> values() {
        final Iterator<Entry<K,V>> entryIterator = entrySet().iterator();
        return new AbstractCollection<V>() {
            @Override
            public Iterator<V> iterator() {
                return new Iterator<V>() {
                    @Override
                    public boolean hasNext() {
                        return entryIterator.hasNext();
                    }

                    @Override
                    public V next() {
                        return entryIterator.next().getValue();
                    }
                };
            }

            @Override
            public int size() {
                return entrySet().size();
            }
        };
    }

    public Iterator<Entry<K,V>> iterator() {
        return iterator(null, false, null, false);
    }

    public Iterator<Entry<K,V>> reverseIterator() {
        return reverseIterator(null, false, null, false);
    }

    protected abstract BTreeEntry<K,V> putEntry(Entry<? extends K, ? extends V> entry);
    protected abstract BTreeEntry<K,V> getEntry(K key);
    protected abstract V removeEntry(K key);
    protected abstract long sizeLong();
    protected abstract Iterator<Entry<K,V>> iterator(K fromKey, boolean fromInclusive, K toKey,
                                                          boolean toInclusive);
    protected abstract Iterator<Entry<K,V>> reverseIterator(K fromKey, boolean fromInclusive, K toKey,
                                                                  boolean toInclusive);

    public static class BTreeEntry<K,V> implements Entry<K,V> {

        private K key;
        private V value;
        private boolean deleted;

        public BTreeEntry(K key, V value, boolean deleted) {
            this.key = key;
            this.value = value;
            this.deleted = deleted;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            this.value = value;
            return value;
        }

        public boolean isDeleted() {
            return deleted;
        }
    }

    public static class SubMap<K, V> extends AbstractBTreeMap<K,V> {

        private final K low, high;
        private final boolean lowInclusive, highInclusive;
        private final AbstractBTreeMap<K,V> parent;

        public SubMap(AbstractBTreeMap<K,V> parent, Comparator<K> comparator, K low, boolean lowInclusive,
                      K high, boolean highInclusive) {
            super(comparator);
            this.parent = parent;
            this.low = low;
            this.high = high;
            this.lowInclusive = lowInclusive;
            this.highInclusive = highInclusive;
        }

        private boolean inBounds(K key) {

            // We don't want null keys!
            if (key == null) {
                return false;
            }
            // If there are no bounds return true
            if (low == null && high == null) {
                return true;
            }
            // If there is no low, compare the high only
            if (low == null && high != null) {
                int cmp = comparator().compare(high, key);
                return highInclusive ? cmp >= 0 : cmp > 0;
            }
            // If there is no high, compare the low only
            if (low != null && high == null) {
                int cmp = comparator().compare(low, key);
                return lowInclusive ? cmp <= 0 : cmp < 0;
            }

            // Otherwise compare both high and low.
            boolean lowInBounds = false, highInbounds = false;
            // Compare low. Key should be high than low
            int lowCmp = comparator().compare(low, key);
            lowInBounds = lowInclusive ? lowCmp <= 0 : lowCmp < 0;
            // Compare high. Key should be lower than high
            int highCmp = comparator().compare(high, key);
            highInbounds = highInclusive ? highCmp >= 0 : highCmp > 0;
            // Both need ot be true
            return lowInBounds && highInbounds;
        }

        @Override
        protected BTreeEntry<K, V> putEntry(Entry<? extends K, ? extends V> entry) {
            if (entry == null) {
                return null;
            }
            inBounds(entry.getKey());
            return parent.putEntry(entry);
        }

        @Override
        protected BTreeEntry<K, V> getEntry(K key) {
            if (!inBounds(key)) {
                return null;
            }
            return parent.getEntry(key);
        }

        @Override
        protected V removeEntry(K key) {
            if (!inBounds(key)) {
                return null;
            }
            return parent.removeEntry(key);
        }

        @Override
        protected long sizeLong() {
            // Fastest way to calculate the size of the map is to iterate to the low and high keys
            long lowCount = low != null ? countUpToKey(low, lowInclusive) : 0;
            long highCount = high != null ? countBackwardsToKey(high, highInclusive) : 0;
            return parent.sizeLong() - (lowCount + highCount);
        }

        private long countUpToKey(K key, boolean inclusive) {
            final AtomicLong count = new AtomicLong();
            for (K i : parent.keySet()) {
                int cmp = comparator().compare(i, key);
                if (cmp == 0) {
                    return inclusive ? count.incrementAndGet() : count.get();
                }
                count.getAndIncrement();
            }
            return count.get();
        }

        private long countBackwardsToKey(K key, boolean inclusive) {
            final AtomicLong count = new AtomicLong();
            for (K i : parent.descendingKeySet()) {
                int cmp = comparator().compare(i, key);
                if (cmp == 0) {
                    return inclusive ? count.incrementAndGet() : count.get();
                }
                count.getAndIncrement();
            }
            return count.get();
        }

        @Override
        public ConcurrentNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
            if ((fromKey != null && !inBounds(fromKey)) && (toKey != null && !inBounds(toKey))) {
                return null;
            }
            return super.subMap(fromKey, fromInclusive, toKey, toInclusive);
        }

        @Override
        public void clear() {
            // FIXME: We should be able to clear a range...
        }

        @Override
        protected Iterator<Entry<K, V>> iterator(K from, boolean fromInclusive, K to, boolean toInclusive) {
            return parent.iterator(from, fromInclusive, to, toInclusive);
        }

        @Override
        protected Iterator<Entry<K, V>> reverseIterator(K from, boolean fromInclusive, K to,
                                                              boolean toInclusive) {
            return parent.reverseIterator(from, fromInclusive, to, toInclusive);
        }
    }

    public static class DescendingMap<K,V> extends SubMap<K,V> {

        public DescendingMap(AbstractBTreeMap<K, V> parent, Comparator<K> comparator, K low, boolean lowInclusive,
                             K high, boolean highInclusive) {
            super(parent, comparator, low, lowInclusive, high, highInclusive);
        }

        @Override
        protected Iterator<Entry<K, V>> iterator(K from, boolean fromInclusive, K to, boolean toInclusive) {
            return super.reverseIterator(from, fromInclusive, to, toInclusive);
        }

        @Override
        protected Iterator<Entry<K, V>> reverseIterator(K from, boolean fromInclusive, K to,
                                                              boolean toInclusive) {
            return super.iterator(from, fromInclusive, to, toInclusive);
        }
    }

    static final class KeySet<K> implements NavigableSet<K> {

        final AbstractBTreeMap<K, ?> map;
        private final K high, low;
        private final boolean lowInclusive, highInclusive;
        private final AtomicLong size = new AtomicLong(-1l);
        private Iterator<K> iterator;

        public KeySet(AbstractBTreeMap<K, ?> map) {
            this.map = map;
            this.low = null;
            this.high = null;
            this.lowInclusive = false;
            this.highInclusive = false;
        }

        public KeySet(AbstractBTreeMap<K, Object> map, K low, boolean lowInclusive, K high, boolean highInclusive) {
            this.map = map;
            this.high = high;
            this.low = low;
            this.highInclusive = highInclusive;
            this.lowInclusive = lowInclusive;
        }

        @Override
        public K lower(K k) {
            return map.lowerKey(k);
        }

        @Override
        public K floor(K k) {
            return map.floorKey(k);
        }

        @Override
        public K ceiling(K k) {
            return map.ceilingKey(k);
        }

        @Override
        public K higher(K k) {
            return map.higherKey(k);
        }

        @Override
        public K pollFirst() {
            Entry<K, ?> entry = map.pollFirstEntry();
            return entry != null ? entry.getKey() : null;
        }

        @Override
        public K pollLast() {
            Entry<K, ?> entry = map.pollLastEntry();
            return entry != null ? entry.getKey() : null;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Iterator<K> iterator() {
            if (iterator == null) {
                final Iterator<Entry<K, Object>> it = ((AbstractBTreeMap<K, Object>)map).iterator(low,
                        lowInclusive, high, highInclusive);
                iterator = new Iterator<K>() {
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public K next() {
                        Entry<K, ?> e = it.next();
                        if (e != null) {
                            return e.getKey();
                        }
                        return null;
                    }
                };
            }
            return iterator;
        }

        @Override
        public NavigableSet<K> descendingSet() {
            return map.descendingKeySet();
        }

        @Override
        public Iterator<K> descendingIterator() {
            return map.descendingKeySet().iterator();
        }

        @Override
        public NavigableSet<K> subSet(K fromElement, boolean fromInclusive, K toElement, boolean toInclusive) {
            return map.subMap(fromElement, fromInclusive, toElement, toInclusive).keySet();
        }

        @Override
        public NavigableSet<K> headSet(K toElement, boolean inclusive) {
            return map.headMap(toElement, inclusive).keySet();
        }

        @Override
        public NavigableSet<K> tailSet(K fromElement, boolean inclusive) {
            return map.tailMap(fromElement, inclusive).keySet();
        }

        @Override
        public SortedSet<K> subSet(K fromElement, K toElement) {
            return map.subMap(fromElement, toElement).keySet();
        }

        @Override
        public SortedSet<K> headSet(K toElement) {
            return map.headMap(toElement).keySet();
        }

        @Override
        public SortedSet<K> tailSet(K fromElement) {
            return map.tailMap(fromElement).keySet();
        }

        @Override
        public Comparator<? super K> comparator() {
            return map.comparator();
        }

        @Override
        public K first() {
            return map.firstKey();
        }

        @Override
        public K last() {
            return map.lastKey();
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public boolean isEmpty() {
            return map.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return map.containsKey(o);
        }

        @Override
        public Object[] toArray() {
            return toArray(new Object[0]);
        }

        @Override
        public <T> T[] toArray(final T[] a) {
            if (a.length >= map.entrySet().size()) {
                final AtomicInteger i = new AtomicInteger();
                map.forEach((k, v) -> {
                    a[i.getAndIncrement()] = (T) k;
                });
            }
            return (T[]) map.entrySet().stream().map((e) -> {
                return e.getKey();
            }).toArray();
        }

        @Override
        public boolean add(K k) {
            return false;
        }

        @Override
        public boolean remove(Object o) {
            return map.remove(o) != null;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return map.entrySet().stream().filter((e) -> c.contains(e.getKey())).count() == c.size();
        }

        @Override
        public boolean addAll(Collection<? extends K> c) {
            return false;
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return false;
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            int count = 0;
            for (Object o : c) {
                map.remove(o);
                count++;
            }
            return count == c.size();
        }

        @Override
        public void clear() {
            map.clear();
        }
    }

    static final class EntrySet<K, V> implements NavigableSet<Entry<K, V>> {

        final AbstractBTreeMap<K, V> map;

        public EntrySet(AbstractBTreeMap<K, V> map) {
            this.map = map;
        }

        @Override
        public Entry<K, V> lower(Entry<K, V> kvEntry) {
            return map.lowerEntry(kvEntry.getKey());
        }

        @Override
        public Entry<K, V> floor(Entry<K, V> kvEntry) {
            return map.floorEntry(kvEntry.getKey());
        }

        @Override
        public Entry<K, V> ceiling(Entry<K, V> kvEntry) {
            return map.ceilingEntry(kvEntry.getKey());
        }

        @Override
        public Entry<K, V> higher(Entry<K, V> kvEntry) {
            return map.higherEntry(kvEntry.getKey());
        }

        @Override
        public Entry<K, V> pollFirst() {
            return map.pollFirstEntry();
        }

        @Override
        public Entry<K, V> pollLast() {
            return map.pollLastEntry();
        }

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return map.iterator();
        }

        @Override
        public NavigableSet<Entry<K, V>> descendingSet() {
            ConcurrentNavigableMap<K, V> map = this.map.descendingMap();
            return new EntrySet<>((AbstractBTreeMap<K, V>)map);
        }

        @Override
        public Iterator<Entry<K, V>> descendingIterator() {
            return map.reverseIterator();
        }

        @Override
        public NavigableSet<Entry<K, V>> subSet(Entry<K, V> fromElement, boolean fromInclusive,
                                                Entry<K, V> toElement, boolean toInclusive) {
            return (NavigableSet<Entry<K, V>>)map.subMap(fromElement != null ? fromElement.getKey() : null,
                    fromInclusive, toElement != null ? toElement.getKey() : null, toInclusive).entrySet();
        }

        @Override
        public NavigableSet<Entry<K, V>> headSet(Entry<K, V> toElement, boolean inclusive) {
            return (NavigableSet<Entry<K, V>>)map.headMap(toElement != null ? toElement.getKey() : null, inclusive)
                    .entrySet();
        }

        @Override
        public NavigableSet<Entry<K, V>> tailSet(Entry<K, V> fromElement, boolean inclusive) {
            return (NavigableSet<Entry<K, V>>)map.tailMap(fromElement != null ? fromElement.getKey() : null, inclusive)
                    .entrySet();
        }

        @Override
        public SortedSet<Entry<K, V>> subSet(Entry<K, V> fromElement, Entry<K, V> toElement) {
            return (NavigableSet<Entry<K, V>>)map.subMap(fromElement != null ? fromElement.getKey() : null,
                    toElement != null ? toElement.getKey() : null).entrySet();
        }

        @Override
        public SortedSet<Entry<K, V>> headSet(Entry<K, V> toElement) {
            return (SortedSet<Entry<K, V>>)map.headMap(toElement != null ? toElement.getKey() : null)
                    .entrySet();
        }

        @Override
        public SortedSet<Entry<K, V>> tailSet(Entry<K, V> fromElement) {
            return (SortedSet<Entry<K, V>>)map.tailMap(fromElement != null ? fromElement.getKey() : null)
                    .entrySet();
        }

        @Override
        public Comparator<? super Entry<K, V>> comparator() {
            return (o1, o2) -> {
                return map.comparator().compare(o1 == null ? null : o1.getKey(), o2 == null ? null : o2.getKey());
            };
        }

        @Override
        public Entry<K, V> first() {
            return map.firstEntry();
        }

        @Override
        public Entry<K, V> last() {
            return map.lastEntry();
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public boolean isEmpty() {
            return map.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            if (o == null) {
                return false;
            }
            if (Map.Entry.class.isAssignableFrom(o.getClass())) {
                return map.containsKey(((Entry<K, V>)o).getKey());
            }
            return map.containsKey(o);
        }

        @Override
        public Object[] toArray() {
            return toArray(new Object[0]);
        }

        @Override
        public <T> T[] toArray(T[] a) {
            if (a.length >= map.entrySet().size()) {
                final AtomicInteger i = new AtomicInteger();
                map.entrySet().stream().forEach((e) -> {
                    a[i.getAndIncrement()] = (T) e;
                });
            }
            return (T[]) map.entrySet().stream().map((e) -> {
                return e;
            }).toArray();
        }

        @Override
        public boolean add(Entry<K, V> kvEntry) {
            return false;
        }

        @Override
        public boolean remove(Object o) {
            if (o == null) {
                return false;
            }
            if (Map.Entry.class.isAssignableFrom(o.getClass())) {
                return map.remove(((Entry<K, V>)o).getKey()) != null;
            }
            return map.remove(o) != null;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return map.entrySet().stream().filter(c::contains).count() == c.size();
        }

        @Override
        public boolean addAll(Collection<? extends Entry<K, V>> c) {
            return false;
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return false;
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            int count = 0;
            for (Object o : c) {
                if (o == null) {
                    continue;
                }
                if (Entry.class.isAssignableFrom(o.getClass())) {
                    map.remove(((Entry) o).getKey());
                }
                map.remove(o);
                count++;
            }
            return count == c.size();
        }

        @Override
        public void clear() {
            map.clear();
        }
    }

    protected static abstract class Node<K, T> {

        protected List<K> keys = new ArrayList<K>();
        protected Comparator<K> comparator;

        public Node(Comparator<K> comparator) {
            this.comparator = comparator;
        }

        public List<K> getKeys() {
            return keys;
        }

        public int findPosition(K key) {
            return Collections.binarySearch(keys, key, comparator);
        }

        public int findNearest(K key) {
            int pos = findPosition(key);
            // If the position is positive, the key exists
            if (pos >= 0) {
                return pos;
            }
            // Key doesn't exist
            pos = -(pos) - 1;
            return pos >= keys.size() ? keys.size() - 1 : pos;
        }

        public abstract void put(K key, T value);

        public abstract T get(K key);

        public abstract Node<K, T>[] split();
    }
}
