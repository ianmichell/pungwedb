package com.pungwe.db.core.collections;

import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.io.store.Store;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ian on 25/05/2016.
 */
public abstract class BaseMap<K, V> implements ConcurrentNavigableMap<K, V> {

    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Comparator<K> keyComparator;

    public BaseMap(Comparator<K> keyComparator) {
        this.keyComparator = keyComparator;
    }

    @Override
    public ConcurrentNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return null;
    }

    @Override
    public ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
        return null;
    }

    @Override
    public ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
        return null;
    }

    @Override
    public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
        return null;
    }

    @Override
    public ConcurrentNavigableMap<K, V> headMap(K toKey) {
        return null;
    }

    @Override
    public ConcurrentNavigableMap<K, V> tailMap(K fromKey) {
        return null;
    }

    @Override
    public ConcurrentNavigableMap<K, V> descendingMap() {
        return null;
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        return null;
    }

    @Override
    public NavigableSet<K> keySet() {
        return null;
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
        return null;
    }

    public abstract Entry<K, V> getEntry(K key);

    public abstract Entry<K, V> putEntry(K key, V value);

    public abstract Entry<K, V> replaceEntry(K key, V value);

    protected abstract Iterator<Entry<K, V>> descendingIterator(Comparator<? super K> comparator,
                                                                K low, boolean lowInclusive,
                                                                K high, boolean highInclusive);

    protected abstract Iterator<Entry<K, V>> iterator(Comparator<? super K> comparator,
                                                      K low, boolean lowInclusive,
                                                      K high, boolean highInclusive);

    @Override
    public Entry<K, V> lowerEntry(K key) {

        Iterator<Entry<K, V>> it = descendingIterator(comparator(), null, true, key, false);
        return it.hasNext() ? it.next() : null;
    }

    @Override
    public Entry<K, V> floorEntry(K key) {

        Iterator<Entry<K, V>> it = descendingIterator(comparator(), null, true, key, true);
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
    public Entry<K, V> ceilingEntry(K key) {

        Iterator<Entry<K, V>> it = iterator(comparator(), key, true, null, true);
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
    public Entry<K, V> higherEntry(K key) {
        Iterator<Entry<K, V>> it = iterator(comparator(), key, false, null, true);
        return it.hasNext() ? it.next() : null;
    }

    @Override
    public Entry<K, V> firstEntry() {
        Iterator<Entry<K, V>> it = iterator(comparator(), null, true, null, true);
        return it.hasNext() ? it.next() : null;
    }

    @Override
    public Entry<K, V> lastEntry() {
        Iterator<Entry<K, V>> it = descendingIterator(comparator(), null, true, null, true);
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
    public K floorKey(K key) {
        Entry<K, V> entry = floorEntry(key);
        if (entry == null) {
            return null;
        }
        return entry.getKey();
    }

    @Override
    public K ceilingKey(K key) {
        Entry<K, V> entry = ceilingEntry(key);
        if (entry == null) {
            return null;
        }
        return entry.getKey();
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
            remove(entry);
        }
        return entry;
    }

    @Override
    public Entry<K, V> pollLastEntry() {
        Entry<K, V> entry = lastEntry();
        if (entry != null) {
            remove(entry);
        }
        return entry;
    }

    @Override
    public Comparator<? super K> comparator() {
        return this.comparator();
    }

    @Override
    public K firstKey() {
        return firstEntry().getKey();
    }

    @Override
    public K lastKey() {
        return lastEntry().getKey();
    }

    @Override
    public V putIfAbsent(K key, V value) {
        if (!containsKey(key)) {
            return null;
        }
        return put(key, value);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        V value = get(key);
        if (value == null || !value.equals(oldValue)) {
            return false;
        }
        return replace(key, value) != null;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        return entrySet().stream().filter(kvEntry -> kvEntry.getValue() != null && kvEntry.getValue().equals(value))
                .findFirst().isPresent();
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> m) {
        if (m == null) {
            return; // do nothing
        }
        m.forEach((k, v) -> {
            put(k, v);
        });
    }

    @Override
    public V replace(K key, V value) {
        Entry<K, V> entry = this.replaceEntry(key, value);
        return entry != null ? entry.getValue() : null;
    }

    @Override
    public V get(Object key) {
        Entry<K, V> entry = getEntry((K) key);
        return entry != null ? entry.getValue() : null;
    }

    @Override
    public V put(K key, V value) {
        return null;
    }

    @Override
    public V remove(Object key) {
        return null;
    }

    @Override
    public Collection<V> values() {
        final Iterator<Entry<K, V>> iterator = entrySet().iterator();
        return new AbstractCollection<V>() {
            @Override
            public Iterator<V> iterator() {
                return new Iterator<V>() {

                    @Override
                    public boolean hasNext() {
                        return iterator().hasNext();
                    }

                    @Override
                    public V next() {
                        Entry<K, V> next = iterator.next();
                        if (next != null) {
                            return next.getValue();
                        }
                        return null;
                    }
                };
            }

            @Override
            public int size() {
                return entrySet().size();
            }
        };
    }

    static final class SubMap<K, V> extends BaseMap<K, V> {

        private final BaseMap<K, V> parent;
        private final K low;
        private final K high;
        private final boolean lowInclusive;
        private final boolean highInclusive;
        private final boolean descending;

        SubMap(BaseMap<K, V> parent, K lowKey, K highKey, boolean lowInclusive,
               boolean highInclusive, boolean descending) {
            super(parent.keyComparator);
            this.parent = parent;
            this.low = lowKey;
            this.high = highKey;
            this.lowInclusive = lowInclusive;
            this.highInclusive = highInclusive;
            this.descending = descending;
            if (low != null && high != null && parent.keyComparator.compare((K) low, (K) high) > 0) {
                throw new IllegalArgumentException();
            }
        }

        private boolean tooLow(K key) {
            if (low != null) {
                int c = parent.keyComparator.compare(key, (K) low);
                if (c > 0 || (c == 0 && !lowInclusive))
                    return true;
            }
            return false;
        }

        private boolean tooHigh(K key) {
            if (high != null) {
                int c = parent.keyComparator.compare(key, (K) high);
                if (c < 0 || (c == 0 && !highInclusive))
                    return true;
            }
            return false;
        }

        private boolean inBounds(K key) {
            return !tooLow(key) && !tooHigh(key);
        }

        @Override
        public Comparator<? super K> comparator() {
            return (o1, o2) -> {
                if (descending) {
                    return -parent.keyComparator.compare(o1, o2);
                }
                return parent.keyComparator.compare(o1, o2);
            };
        }

        @Override
        protected Iterator<Entry<K, V>> descendingIterator(Comparator<? super K> comparator,
                                                           K low, boolean lowInclusive,
                                                           K high, boolean highInclusive) {
            return parent.descendingIterator(comparator, low, lowInclusive, high, highInclusive);
        }

        @Override
        protected Iterator<Entry<K, V>> iterator(Comparator<? super K> comparator,
                                                 K low, boolean lowInclusive,
                                                 K high, boolean highInclusive) {
            return parent.iterator(comparator, low, lowInclusive, high, highInclusive);
        }

        @Override
        public Entry<K, V> lowerEntry(K key) {

            if (!inBounds(key)) {
                return null;
            }

            Iterator<Entry<K, V>> it = descendingIterator(comparator(), low, true, key, false);
            return it.hasNext() ? it.next() : null;
        }

        @Override
        public Entry<K, V> floorEntry(K key) {

            if (!inBounds(key)) {
                return null;
            }

            Iterator<Entry<K, V>> it = descendingIterator(comparator(), low, true, key, true);
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
        public Entry<K, V> ceilingEntry(K key) {

            if (!inBounds(key)) {
                return null;
            }

            Iterator<Entry<K, V>> it = iterator(comparator(), key, true, high, true);
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
        public Entry<K, V> higherEntry(K key) {
            if (!inBounds(key)) {
                return null;
            }

            Iterator<Entry<K, V>> it = iterator(comparator(), key, false, high, true);
            return it.hasNext() ? it.next() : null;
        }

        @Override
        public Entry<K, V> firstEntry() {
            Iterator<Entry<K, V>> it = iterator(comparator(), low, lowInclusive, high, highInclusive);
            return it.hasNext() ? it.next() : null;
        }

        @Override
        public Entry<K, V> lastEntry() {
            Iterator<Entry<K, V>> it = descendingIterator(comparator(), high, highInclusive, low, lowInclusive);
            return it.hasNext() ? it.next() : null;
        }

        @Override
        public boolean remove(Object key, Object value) {
            return false;
        }

        @Override
        public Entry<K, V> getEntry(K key) {
            return !inBounds(key) ? null : parent.getEntry(key);
        }

        @Override
        public Entry<K, V> putEntry(K key, V value) {
            return !inBounds(key) ? null : parent.putEntry(key, value);
        }

        @Override
        public Entry<K, V> replaceEntry(K key, V value) {
            return !inBounds(key) ? null : parent.replaceEntry(key, value);
        }

        @Override
        public void clear() {
            // do nothing
        }

        @Override
        public int size() {
            return keySet().size();
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            return null;
        }
    }

    static final class KeySet<K> implements NavigableSet<K> {

        final EntrySet<K, Object> entrySet;

        public KeySet(EntrySet<K, Object> entrySet) {
            this.entrySet = entrySet;
        }

        @Override
        public K lower(K k) {
            return null;
        }

        @Override
        public K floor(K k) {
            return null;
        }

        @Override
        public K ceiling(K k) {
            return null;
        }

        @Override
        public K higher(K k) {
            return null;
        }

        @Override
        public K pollFirst() {
            return null;
        }

        @Override
        public K pollLast() {
            return null;
        }

        @Override
        public Iterator<K> iterator() {
            return null;
        }

        @Override
        public NavigableSet<K> descendingSet() {
            return null;
        }

        @Override
        public Iterator<K> descendingIterator() {
            return null;
        }

        @Override
        public NavigableSet<K> subSet(K fromElement, boolean fromInclusive, K toElement, boolean toInclusive) {
            return null;
        }

        @Override
        public NavigableSet<K> headSet(K toElement, boolean inclusive) {
            return null;
        }

        @Override
        public NavigableSet<K> tailSet(K fromElement, boolean inclusive) {
            return null;
        }

        @Override
        public SortedSet<K> subSet(K fromElement, K toElement) {
            return null;
        }

        @Override
        public SortedSet<K> headSet(K toElement) {
            return null;
        }

        @Override
        public SortedSet<K> tailSet(K fromElement) {
            return null;
        }

        @Override
        public Comparator<? super K> comparator() {
            return null;
        }

        @Override
        public K first() {
            return null;
        }

        @Override
        public K last() {
            return null;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean contains(Object o) {
            return false;
        }

        @Override
        public Object[] toArray() {
            return new Object[0];
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return null;
        }

        @Override
        public boolean add(K k) {
            return false;
        }

        @Override
        public boolean remove(Object o) {
            return false;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return false;
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
            return false;
        }

        @Override
        public void clear() {

        }
    }

    static final class EntrySet<K, V> implements NavigableSet<Entry<K, V>> {

        final BaseMap<K, V> map;
        final Entry<K, V> high, low;
        final boolean highInclusive, lowInclusive;

        public EntrySet(BaseMap<K, V> map, Entry<K, V> high, Entry<K, V> low, boolean highInclusive, boolean lowInclusive) {
            this.map = map;
            this.high = high;
            this.low = low;
            this.highInclusive = highInclusive;
            this.lowInclusive = lowInclusive;
        }

        @Override
        public Entry<K, V> lower(Entry<K, V> kvEntry) {
            return null;
        }

        @Override
        public Entry<K, V> floor(Entry<K, V> kvEntry) {
            return null;
        }

        @Override
        public Entry<K, V> ceiling(Entry<K, V> kvEntry) {
            return null;
        }

        @Override
        public Entry<K, V> higher(Entry<K, V> kvEntry) {
            return null;
        }

        @Override
        public Entry<K, V> pollFirst() {
            return null;
        }

        @Override
        public Entry<K, V> pollLast() {
            return null;
        }

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return null;
        }

        @Override
        public NavigableSet<Entry<K, V>> descendingSet() {
            return null;
        }

        @Override
        public Iterator<Entry<K, V>> descendingIterator() {
            return null;
        }

        @Override
        public NavigableSet<Entry<K, V>> subSet(Entry<K, V> fromElement, boolean fromInclusive, Entry<K, V> toElement, boolean toInclusive) {
            return null;
        }

        @Override
        public NavigableSet<Entry<K, V>> headSet(Entry<K, V> toElement, boolean inclusive) {
            return null;
        }

        @Override
        public NavigableSet<Entry<K, V>> tailSet(Entry<K, V> fromElement, boolean inclusive) {
            return null;
        }

        @Override
        public SortedSet<Entry<K, V>> subSet(Entry<K, V> fromElement, Entry<K, V> toElement) {
            return null;
        }

        @Override
        public SortedSet<Entry<K, V>> headSet(Entry<K, V> toElement) {
            return null;
        }

        @Override
        public SortedSet<Entry<K, V>> tailSet(Entry<K, V> fromElement) {
            return null;
        }

        @Override
        public Comparator<? super Entry<K, V>> comparator() {
            return null;
        }

        @Override
        public Entry<K, V> first() {
            return null;
        }

        @Override
        public Entry<K, V> last() {
            return null;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean contains(Object o) {
            return false;
        }

        @Override
        public Object[] toArray() {
            return new Object[0];
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return null;
        }

        @Override
        public boolean add(Entry<K, V> kvEntry) {
            return false;
        }

        @Override
        public boolean remove(Object o) {
            return false;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return false;
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
            return false;
        }

        @Override
        public void clear() {

        }
    }
}
