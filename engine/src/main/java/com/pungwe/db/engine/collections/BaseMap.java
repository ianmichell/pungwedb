package com.pungwe.db.engine.collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ian on 25/05/2016.
 */
public abstract class BaseMap<K, V> implements ConcurrentNavigableMap<K, V> {

    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Comparator<K> keyComparator;

    public BaseMap(Comparator<K> keyComparator) {
        this.keyComparator = keyComparator;
    }

    @Override
    public ConcurrentNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return new SubMap<>(this, fromKey, toKey, fromInclusive, toInclusive, false);
    }

    @Override
    public ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
        return new SubMap<>(this, null, toKey, true, inclusive, false);
    }

    @Override
    public ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
        return new SubMap<>(this, fromKey, null, inclusive, true, false);
    }

    @Override
    public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
        return subMap(fromKey, true, toKey, false);
    }

    @Override
    public ConcurrentNavigableMap<K, V> headMap(K toKey) {
        return headMap(toKey, false);
    }

    @Override
    public ConcurrentNavigableMap<K, V> tailMap(K fromKey) {
        return tailMap(fromKey, false);
    }

    @Override
    public ConcurrentNavigableMap<K, V> descendingMap() {
        return new SubMap<>(this, null, null, true, true, true);
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        return new KeySet<>((BaseMap<K, Object>)this);
    }

    @Override
    public NavigableSet<K> keySet() {
        return navigableKeySet();
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
        return this.descendingMap().navigableKeySet();
    }

    public abstract Entry<K, V> getEntry(K key);

    public Entry<K, V> putEntry(K key, V value) {
        return putEntry(key, value, true);
    }

    public abstract Entry<K, V> putEntry(K key, V value, boolean replace);

    public Entry<K, V> replaceEntry(K key, V value) {
        Entry<K, V> entry = getEntry(key);
        putEntry(key, value, true);
        return entry;
    }

    protected abstract Iterator<Entry<K, V>> descendingIterator(Comparator<? super K> comparator,
                                                                K low, boolean lowInclusive,
                                                                K high, boolean highInclusive);

    protected Iterator<Entry<K, V>> descendingIterator() {
        return descendingIterator((o1, o2) ->
                -comparator().compare(o1, o2), null, true, null, true);
    }

    protected abstract Iterator<Entry<K, V>> iterator(Comparator<? super K> comparator,
                                                      K low, boolean lowInclusive,
                                                      K high, boolean highInclusive);

    protected Iterator<Entry<K, V>> iterator() {
        return iterator(comparator(), null, true, null, true);
    }

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
        return keyComparator;
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
        Entry<K, V> entry = putEntry(key, value);
        return entry == null ? null : entry.getValue();
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

    final static class BaseMapEntry<K, V> implements Map.Entry<K, V> {

        static final Logger log = LoggerFactory.getLogger(BaseMapEntry.class);

        private final BaseMap<K, V> map;
        private final K key;
        private final V value;

        public BaseMapEntry(K key, V value, BaseMap<K, V> map) {
            this.key = key;
            this.value = value;
            this.map = map;
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
            Entry<K, V> e = map.putEntry((K)key, value, true);
            if (e != null) {
                return e.getValue();
            }
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            BaseMapEntry<K,V> that = (BaseMapEntry<K,V>) o;

            if (!key.equals(that.key)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }
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
            if (low != null && high != null && comparator().compare((K) low, (K) high) > 0) {
                throw new IllegalArgumentException();
            }
        }

        private boolean tooLow(K key) {
            if (low != null) {
                int c = comparator().compare(key, low);
                if (c < 0 || (c == 0 && !lowInclusive))
                    return true;
            }
            return false;
        }

        private boolean tooHigh(K key) {
            if (high != null) {
                int c = comparator().compare(key, (K) high);
                if (c > 0 || (c == 0 && !highInclusive))
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
            if (descending) {
                return parent.iterator(comparator, low, lowInclusive, high, highInclusive);
            } else {
                return parent.descendingIterator((o1, o2) -> -comparator.compare(o1, o2), low, lowInclusive,
                        high, highInclusive);
            }
        }

        @Override
        protected Iterator<Entry<K, V>> iterator(Comparator<? super K> comparator,
                                                 K low, boolean lowInclusive,
                                                 K high, boolean highInclusive) {
            if (descending) {
                return parent.descendingIterator((o1, o2) -> -comparator.compare(o1, o2), low, lowInclusive,
                        high, highInclusive);
            } else {
                return parent.iterator(comparator, low, lowInclusive, high, highInclusive);
            }
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
        public Entry<K, V> putEntry(K key, V value, boolean replace) {
            return !inBounds(key) ? null : parent.putEntry(key, value, replace);
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
            return new KeySet<K>((BaseMap<K, Object>) parent, low, lowInclusive, high, highInclusive).size();
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            return new EntrySet<>(this);
        }

        @Override
        public NavigableSet<K> keySet() {
            return new KeySet<>((BaseMap<K, Object>) this, low, lowInclusive, high, highInclusive);
        }
    }

    static final class KeySet<K> implements NavigableSet<K> {

        final BaseMap<K, Object> map;
        private final K high, low;
        private final boolean lowInclusive, highInclusive;
        private final AtomicLong size = new AtomicLong(-1l);
        private Iterator<K> iterator;

        public KeySet(BaseMap<K, Object> map) {
            this.map = map;
            this.low = null;
            this.high = null;
            this.lowInclusive = false;
            this.highInclusive = false;
        }

        public KeySet(BaseMap<K, Object> map, K low, boolean lowInclusive, K high, boolean highInclusive) {
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
            Entry<K, Object> entry = map.pollFirstEntry();
            return entry != null ? entry.getKey() : null;
        }

        @Override
        public K pollLast() {
            Entry<K, Object> entry = map.pollLastEntry();
            return entry != null ? entry.getKey() : null;
        }

        @Override
        public Iterator<K> iterator() {

            if (iterator == null) {
                final Iterator<Entry<K, Object>> it = map.iterator(map.comparator(), low, lowInclusive,
                        high, highInclusive);

                iterator = new Iterator<K>() {
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public K next() {
                        Entry<K, Object> e = it.next();
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

        final BaseMap<K, V> map;

        public EntrySet(BaseMap<K, V> map) {
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
            return new EntrySet<K, V>((BaseMap<K, V>)map);
        }

        @Override
        public Iterator<Entry<K, V>> descendingIterator() {
            return map.descendingIterator();
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
            return map.entrySet().stream().filter((e) -> c.contains(e)).count() == c.size();
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
}
