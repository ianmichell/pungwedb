package com.pungwe.db.engine.collections.btree;

import com.pungwe.db.engine.collections.btree.BTreeMap;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Created by ian on 08/07/2016.
 */
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
        long time = end - start;
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        final AtomicInteger counter = new AtomicInteger();
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
        long time = end - start;
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
        long time = end - start;
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
        long time = end - start;
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        final AtomicInteger counter = new AtomicInteger(10000);
        Iterator<Map.Entry<Long, Long>> it = map.descendingMap().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Long> entry = it.next();
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
        long time = end - start;
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        final AtomicInteger counter = new AtomicInteger(0);
        Iterator<Map.Entry<Long, Long>> it = map.descendingMap().descendingMap().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Long> entry = it.next();
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
        long time = end - start;
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59l, true, 513l, true);
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
        long time = end - start;
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59l, false, 513l, false);
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
        long time = end - start;
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59l, false, 513l, true);
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
        long time = end - start;
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59l, true, 513l, false);
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
        long time = end - start;
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59l, true, 513l, true);
        AtomicLong counter = new AtomicLong(59l);
        Iterator<Map.Entry<Long, Long>> it = sub.entrySet().iterator();
        while (it.hasNext()) {
            assertEquals(new Long(counter.getAndIncrement()), it.next().getKey());
        }
        assertEquals(514l, counter.get());
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
        long time = end - start;
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59l, false, 513l, false);
        AtomicLong counter = new AtomicLong(60);
        Iterator<Map.Entry<Long, Long>> it = sub.entrySet().iterator();
        while (it.hasNext()) {
            assertEquals(new Long(counter.getAndIncrement()), it.next().getKey());
        }
        assertEquals(513l, counter.get());
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
        long time = end - start;
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59l, false, 513l, true);
        AtomicLong counter = new AtomicLong(60);
        Iterator<Map.Entry<Long, Long>> it = sub.entrySet().iterator();
        while (it.hasNext()) {
            assertEquals(new Long(counter.getAndIncrement()), it.next().getKey());
        }
        assertEquals(514l, counter.get());
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
        long time = end - start;
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59l, true, 513l, false);
        AtomicLong counter = new AtomicLong(59);
        Iterator<Map.Entry<Long, Long>> it = sub.entrySet().iterator();
        while (it.hasNext()) {
            assertEquals(new Long(counter.getAndIncrement()), it.next().getKey());
        }
        assertEquals(513l, counter.get());
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
        long time = end - start;
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59l, true, 513l, true);
        AtomicLong counter = new AtomicLong(513);
        Iterator<Map.Entry<Long, Long>> it = sub.descendingMap().entrySet().iterator();
        while (it.hasNext()) {
            assertEquals(new Long(counter.getAndDecrement()), it.next().getKey());
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
        long time = end - start;
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59l, false, 513l, false);
        AtomicLong counter = new AtomicLong(512);
        Iterator<Map.Entry<Long, Long>> it = sub.descendingMap().entrySet().iterator();
        while (it.hasNext()) {
            assertEquals(new Long(counter.getAndDecrement()), it.next().getKey());
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
        long time = end - start;
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59l, false, 513l, true);
        AtomicLong counter = new AtomicLong(513);
        Iterator<Map.Entry<Long, Long>> it = sub.descendingMap().entrySet().iterator();
        while (it.hasNext()) {
            assertEquals(new Long(counter.getAndDecrement()), it.next().getKey());
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
        long time = end - start;
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        // Iterate a million records
        ConcurrentNavigableMap<Long, Long> sub = map.subMap(59l, true, 513l, false);
        AtomicLong counter = new AtomicLong(512);
        Iterator<Map.Entry<Long, Long>> it = sub.descendingMap().entrySet().iterator();
        while (it.hasNext()) {
            assertEquals(new Long(counter.getAndDecrement()), it.next().getKey());
        }
        assertEquals(58, counter.get());
    }
}
