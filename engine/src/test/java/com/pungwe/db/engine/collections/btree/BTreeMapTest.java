package com.pungwe.db.engine.collections.btree;

import org.junit.Test;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class BTreeMapTest {

    @Test
    public void testSplitAndGet() throws Exception {

        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
        long start = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        for (long i = 0; i < 10000; i++) {
            assertEquals(new Long(i), map.get(i));
        }
    }

    @Test
    public void testSplitAndIterate() throws Exception {
        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
        long start = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        final AtomicInteger counter = new AtomicInteger();
        Iterator<Map.Entry<Long, Long>> it = map.iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Long> entry = it.next();
            assertNotNull(entry);
            assertEquals(new Long(counter.getAndIncrement()), entry.getKey());
        }
        assertNull(it.next()); // Assert that the value is now null and we don't progress
        assertEquals(10000, counter.longValue());
    }

    @Test
    public void testSplitAndIterateBackwards() throws Exception {
        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
        long start = System.nanoTime();
        for (long i = 10000; i >= 0; i--) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        final AtomicInteger counter = new AtomicInteger(10000);
        Iterator<Map.Entry<Long, Long>> it = map.reverseIterator();
        while (it.hasNext()) {
            Map.Entry<Long, Long> entry = it.next();
            assertNotNull(entry);
            assertEquals(new Long(counter.getAndDecrement()), entry.getKey());
        }
        assertEquals(-1, counter.longValue());
    }

    @Test
    public void testSplitAndReverseMapIterate() throws Exception {
        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
        long start = System.nanoTime();
        for (long i = 10000; i >= 0; i--) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        final AtomicInteger counter = new AtomicInteger(10000);
        for (Map.Entry<Long, Long> entry : map.descendingMap().entrySet()) {
            assertNotNull(entry);
            assertEquals(new Long(counter.getAndDecrement()), entry.getKey());
        }
        assertEquals(-1, counter.longValue());
    }

    @Test
    public void testSplitAndReverseMapIterateBackwards() throws Exception {
        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
        long start = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        final AtomicInteger counter = new AtomicInteger(0);
        for (Map.Entry<Long, Long> entry : map.descendingMap().descendingMap().entrySet()) {
            assertNotNull(entry);
            assertEquals(new Long(counter.getAndIncrement()), entry.getKey());
        }
        assertEquals(10000, counter.longValue());
    }

    //###########################################################################################
    //#                          SUB MAPS                                                       #
    //###########################################################################################

    @Test
    public void testSubMapGet() throws Exception {

        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
        long start = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59L, true, 513L, true);
        for (long i = 59; i < 514; i++) {
            assertEquals(new Long(i), sub.get(i));
        }
    }

    @Test
    public void testSubMapGetExclusive() throws Exception {

        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
        long start = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59L, false, 513L, false);
        for (long i = 59; i < 514; i++) {
            if (i == 59 || i == 513) {
                assertNull(sub.get(i));
                continue;
            }
            assertEquals(new Long(i), sub.get(i));
        }
    }

    @Test
    public void testSubMapGetFromExclusive() throws Exception {

        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
        long start = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59L, false, 513L, true);
        for (long i = 59; i < 514; i++) {
            if (i == 59) {
                assertNull(sub.get(i));
                continue;
            }
            assertEquals(new Long(i), sub.get(i));
        }
    }

    @Test
    public void testSubMapGetToExclusive() throws Exception {

        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
        long start = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59L, true, 513L, false);
        for (long i = 59; i < 514; i++) {
            if (i == 513) {
                assertNull(sub.get(i));
                continue;
            }
            assertEquals(new Long(i), sub.get(i));
        }
    }

    //###########################################################################################
    //#                          ITERATORS                                                      #
    //###########################################################################################

    @Test
    public void testSubMapIterator() throws Exception {

        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
        long start = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59L, true, 513L, true);
        AtomicLong counter = new AtomicLong(59L);
        for (Map.Entry<Long, Long> longLongEntry : sub.entrySet()) {
            assertEquals(new Long(counter.getAndIncrement()), longLongEntry.getKey());
        }
        assertEquals(514L, counter.get());
    }

    @Test
    public void testSubMapIteratorExclusive() throws Exception {

        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
        long start = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59L, false, 513L, false);
        AtomicLong counter = new AtomicLong(60);
        for (Map.Entry<Long, Long> longLongEntry : sub.entrySet()) {
            assertEquals(new Long(counter.getAndIncrement()), longLongEntry.getKey());
        }
        assertEquals(513L, counter.get());
    }

    @Test
    public void testSubMapIteratorFromExclusive() throws Exception {

        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
        long start = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59L, false, 513L, true);
        AtomicLong counter = new AtomicLong(60);
        for (Map.Entry<Long, Long> longLongEntry : sub.entrySet()) {
            assertEquals(new Long(counter.getAndIncrement()), longLongEntry.getKey());
        }
        assertEquals(514L, counter.get());
    }

    @Test
    public void testSubMapIteratorToExclusive() throws Exception {

        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
        long start = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59L, true, 513L, false);
        AtomicLong counter = new AtomicLong(59);
        for (Map.Entry<Long, Long> longLongEntry : sub.entrySet()) {
            assertEquals(new Long(counter.getAndIncrement()), longLongEntry.getKey());
        }
        assertEquals(513L, counter.get());
    }

    //###########################################################################################
    //#                         REVERSE ITERATORS                                               #
    //###########################################################################################

    @Test
    public void testSubMapReverseIterator() throws Exception {

        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
        long start = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59L, true, 513L, true);
        AtomicLong counter = new AtomicLong(513);
        for (Map.Entry<Long, Long> longLongEntry : sub.descendingMap().entrySet()) {
            assertEquals(new Long(counter.getAndDecrement()), longLongEntry.getKey());
        }
        assertEquals(58, counter.get());
    }

    @Test
    public void testSubMapReverseIteratorExclusive() throws Exception {

        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
        long start = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59L, false, 513L, false);
        AtomicLong counter = new AtomicLong(512);
        for (Map.Entry<Long, Long> longLongEntry : sub.descendingMap().entrySet()) {
            assertEquals(new Long(counter.getAndDecrement()), longLongEntry.getKey());
        }
        assertEquals(59, counter.get());
    }

    @Test
    public void testSubMapReverseIteratorFromExclusive() throws Exception {

        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
        long start = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59L, false, 513L, true);
        AtomicLong counter = new AtomicLong(513);
        for (Map.Entry<Long, Long> longLongEntry : sub.descendingMap().entrySet()) {
            assertEquals(new Long(counter.getAndDecrement()), longLongEntry.getKey());
        }
        assertEquals(59, counter.get());
    }

    @Test
    public void testSubMapReverseIteratorToExclusive() throws Exception {

        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
        long start = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59L, true, 513L, false);
        AtomicLong counter = new AtomicLong(512);
        for (Map.Entry<Long, Long> longLongEntry : sub.descendingMap().entrySet()) {
            assertEquals(new Long(counter.getAndDecrement()), longLongEntry.getKey());
        }
        assertEquals(58, counter.get());
    }

    //###########################################################################################
    //#                         ARBITRARY KEYS                                                  #
    //##########################################################################################

    @SuppressWarnings("unchecked")
    @Test
    public void testArbitraryKeys() throws Exception {

        BTreeMap<Object, Object> map = new BTreeMap<>((o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Cannot contain null keys");
            }
            // Compare numbers
            if (Number.class.isAssignableFrom(o1.getClass()) && Number.class.isAssignableFrom(o2.getClass())) {
                return new BigDecimal(o1.toString()).compareTo(new BigDecimal(o2.toString()));
            }
            // If String
            if (String.class.isAssignableFrom(o1.getClass()) && String.class.isAssignableFrom(o2.getClass())) {
                return ((String)o1).compareTo((String)o2);
            }
            // Boolean
            if (Boolean.class.isAssignableFrom(o1.getClass()) && Boolean.class.isAssignableFrom(o2.getClass())) {
                return ((Boolean)o1).compareTo((Boolean)o2);
            }
            // Everything else...
            return Integer.compare(o1.hashCode(), o2.hashCode());
        }, 10);

        map.put("key", "value");
        map.put(1, 1);
        map.put(2, 2);
        Map<String, Object> key1 = new LinkedHashMap<>();
        key1.put("key", "value");
        Map<String, Object> key2 = new LinkedHashMap<>();
        key2.put("another key", "value");
        map.put(key1, key1);
        map.put(key2, key2);

        Iterator<Map.Entry<Object, Object>> it = map.iterator();
        while (it.hasNext()) {
            System.out.println(it.next());
        }

        assertEquals("value", map.get("key"));
        assertEquals(1, map.get(1));
        assertEquals(2, map.get(2));
        assertEquals(key1, map.get(key1));
        assertEquals(key2, map.get(key2));
    }
}
