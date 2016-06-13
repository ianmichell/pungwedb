package com.pungwe.db.core.collections;

import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.io.store.Store;
import com.pungwe.db.core.io.store.DirectStore;
import com.pungwe.db.core.io.volume.Volume;
import com.pungwe.db.core.io.volume.HeapByteBufferVolume;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.io.IOException;

import static javafx.scene.input.KeyCode.V;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by ian on 27/05/2016.
 */
public class BTreeMapTest {

    private Volume volume;
    private Store store;

    @Before
    public void beforeTest() throws IOException {
        volume = new HeapByteBufferVolume("memory", false, 20, -1l);
        store = new DirectStore(volume);
    }

    @Test
    public void testPut() throws IOException {

        BTreeMap<String, String> map = new BTreeMap<String, String>(store, (o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Keys cannot be null");
            }
            return o1.compareToIgnoreCase(o2);
        }, new ObjectSerializer(), new ObjectSerializer(), 5, -1);

        map.put("key", "value");

        assertEquals(1, store.size());
        assertEquals("value", map.get("key"));
    }

    @Test
    public void testSplit() throws IOException {

        BTreeMap<String, String> map = new BTreeMap<String, String>(store, (o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Keys cannot be null");
            }
            return o1.compareToIgnoreCase(o2);
        }, new ObjectSerializer(), new ObjectSerializer(), 5, -1);

        map.put("1", "1");
        map.put("2", "2");
        map.put("3", "3");
        map.put("4", "4");
        map.put("5", "5");
        map.put("6", "6");

        assertEquals(3, store.size());
        assertEquals("1", map.get("1"));
        assertEquals("2", map.get("2"));
        assertEquals("3", map.get("3"));
        assertEquals("4", map.get("4"));
        assertEquals("5", map.get("5"));
        assertEquals("6", map.get("6"));
    }

    @Test
    public void testMany() throws Exception {
        final BTreeMap<Long, Long> map = new BTreeMap<>(store, (o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Keys cannot be null");
            }
            return o1.compareTo(o2);
        }, new ObjectSerializer(), new ObjectSerializer(), 5, -1);

        long start = System.nanoTime();
        for (int i = 0; i < 100; i++) {
            final long key = i;
            map.put(key, key);
        }
        // Timeout after 1 minute...
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        System.out.println("Tree Size: " + store.size());
        for (int i = 0; i < 100; i++) {
            try {
                long get = map.get((long) i);
                assertEquals((long) i, get);
            } catch (Throwable ex) {
                System.out.println("Failed at record: " + i);
                throw ex;
            }
        }
    }

    @Test
    public void testMultiThread() throws Exception {

        final BTreeMap<Long, Long> map = new BTreeMap<>(store, (o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Keys cannot be null");
            }
            return o1.compareTo(o2);
        }, new ObjectSerializer(), new ObjectSerializer(), 5, -1);

        ExecutorService executor = Executors.newFixedThreadPool(8);

        List<Callable<Long>> threads = new LinkedList<>();
        try {
            for (int i = 0; i < 100; i++) {
                final long key = i;
                threads.add(() -> map.put(key, key));
            }
            // Timeout after 1 minute...
            long start = System.nanoTime();
            executor.invokeAll(threads, 1, TimeUnit.MINUTES);
            long end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

            System.out.println("Tree Size: " + store.size());
            for (int i = 0; i < 100; i++) {
                try {
                    long get = map.get((long) i);
                    assertEquals((long) i, get);
                } catch (Throwable ex) {
                    System.out.println("Failed at record: " + i);
                    throw ex;
                }
            }
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testMultiInsertRandom() throws Exception {

        final BTreeMap<String, String> map = new BTreeMap<>(store, (o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Keys cannot be null");
            }
            return o1.compareTo(o2);
        }, new ObjectSerializer(), new ObjectSerializer(), 200, -1);

        List<String> guids = new LinkedList<>();
        // Timeout after 1 minute...
        long start = System.nanoTime();
        for (int i = 0; i < 100; i++) {
            final String key = UUID.randomUUID().toString();
            guids.add(key);
            map.put(key, key);
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put: " + store.size(), (end - start) / 1000000d));

        System.out.println("Tree Size: " + store.size());
        for (String guid : guids) {
            assertEquals(guid, map.get(guid));
        }
    }

    @Test
    public void testMultiThreadInsertRandom() throws Exception {

        final BTreeMap<String, String> map = new BTreeMap<>(store, (o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Keys cannot be null");
            }
            return o1.compareTo(o2);
        }, new ObjectSerializer(), new ObjectSerializer(), 5, -1);

        ExecutorService executor = Executors.newFixedThreadPool(8);

        List<Callable<String>> threads = new LinkedList<>();
        List<String> guids = new LinkedList<>();
        try {
            for (int i = 0; i < 100; i++) {
                final String key = UUID.randomUUID().toString();
                guids.add(key);
                threads.add(() -> map.put(key, key));
            }
            // Timeout after 1 minute...
            long start = System.nanoTime();
            executor.invokeAll(threads, 1, TimeUnit.MINUTES);
            long end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to put " + store.size(), (end - start) / 1000000d));

            System.out.println("Tree Size: " + store.size());
            for (String guid : guids) {
                assertEquals(guid, map.get(guid));
            }
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testIterateEntries() throws IOException {
        final BTreeMap<String, String> map = new BTreeMap<>(store, (o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Keys cannot be null");
            }
            return o1.compareTo(o2);
        }, new ObjectSerializer(), new ObjectSerializer(), 5, -1);

        map.put("1", "1");
        map.put("2", "2");
        map.put("3", "3");
        map.put("4", "4");

        int i = 1;
        Iterator<Map.Entry<String, String>> it = map.iterator();
        while (it.hasNext()) {
            assertEquals(i + "", it.next().getKey());
            i++;
        }
    }

    @Test
    public void testIterateBackwards() throws IOException {
        final BTreeMap<String, String> map = new BTreeMap<>(store, (o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Keys cannot be null");
            }
            return o1.compareTo(o2);
        }, new ObjectSerializer(), new ObjectSerializer(), 5, -1);

        map.put("1", "1");
        map.put("2", "2");
        map.put("3", "3");
        map.put("4", "4");

        int i = 4;
        Iterator<Map.Entry<String, String>> it = map.descendingIterator();
        while (it.hasNext()) {
            assertEquals(i + "", it.next().getKey());
            i--;
        }
    }

    @Test
    public void testReverseMap() throws IOException {
        final BTreeMap<String, String> map = new BTreeMap<>(store, (o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Keys cannot be null");
            }
            return o1.compareTo(o2);
        }, new ObjectSerializer(), new ObjectSerializer(), 5, -1);

        map.put("1", "1");
        map.put("2", "2");
        map.put("3", "3");
        map.put("4", "4");

        BaseMap<String, String> reverseMap = (BaseMap<String, String>)map.descendingMap();
        assertEquals("4", reverseMap.firstKey());
        assertEquals("1", reverseMap.lastKey());
    }

    @Test
    public void testIteratingReverseMap() throws IOException {
        final BTreeMap<String, String> map = new BTreeMap<>(store, (o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Keys cannot be null");
            }
            return o1.compareTo(o2);
        }, new ObjectSerializer(), new ObjectSerializer(), 5, -1);

        map.put("1", "1");
        map.put("2", "2");
        map.put("3", "3");
        map.put("4", "4");

        BaseMap<String, String> reverseMap = (BaseMap<String, String>)map.descendingMap();

        int i = 4;
        Iterator<Map.Entry<String, String>> it = reverseMap.iterator();
        while (it.hasNext()) {
            assertEquals(i + "", it.next().getKey());
            i--;
        }
    }

    @Test
    public void testReverseIteratingReverseMap() throws IOException {
        final BTreeMap<String, String> map = new BTreeMap<>(store, (o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Keys cannot be null");
            }
            return o1.compareTo(o2);
        }, new ObjectSerializer(), new ObjectSerializer(), 5, -1);

        map.put("1", "1");
        map.put("2", "2");
        map.put("3", "3");
        map.put("4", "4");

        BaseMap<String, String> reverseMap = (BaseMap<String, String>)map.descendingMap();

        int i = 1;
        Iterator<Map.Entry<String, String>> it = reverseMap.descendingIterator();
        while (it.hasNext()) {
            assertEquals(i + "", it.next().getKey());
            i++;
        }
    }

    @Test
    public void testSubMap() throws IOException {

        final BTreeMap<Long, Long> map = new BTreeMap<>(store, (o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Keys cannot be null");
            }
            return o1.compareTo(o2);
        }, new ObjectSerializer(), new ObjectSerializer(), 5, -1);

        for (long i = 1; i < 101; i++) {
            map.put(i, i);
        }

        BaseMap<Long, Long> subMap = (BaseMap<Long, Long>)map.subMap(5l, true, 25l, true);
        long i = 5;
        for (long key : subMap.keySet()) {
            assertEquals(i++, key);
        }
    }

    @Test
    public void testHeadMapInclusive() throws IOException {

        final BTreeMap<Long, Long> map = new BTreeMap<>(store, (o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Keys cannot be null");
            }
            return o1.compareTo(o2);
        }, new ObjectSerializer(), new ObjectSerializer(), 5, -1);

        for (long i = 1; i < 101; i++) {
            map.put(i, i);
        }

        BaseMap<Long, Long> subMap = (BaseMap<Long, Long>)map.headMap(5l, true);
        for (long key = 1; key < 6; key++) {
            assertEquals(key, (long)subMap.get(key));
        }
    }

    @Test
    public void testHeadMap() throws IOException {

        final BTreeMap<Long, Long> map = new BTreeMap<>(store, (o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Keys cannot be null");
            }
            return o1.compareTo(o2);
        }, new ObjectSerializer(), new ObjectSerializer(), 5, -1);

        for (long i = 1; i < 101; i++) {
            map.put(i, i);
        }

        BaseMap<Long, Long> subMap = (BaseMap<Long, Long>)map.headMap(5l, false);
        for (long key = 1; key < 5; key++) {
            assertEquals(key, (long)subMap.get(key));
        }
        assertNull(subMap.get(6l));
    }
    @Test
    public void testTailMap() throws IOException {

        final BTreeMap<Long, Long> map = new BTreeMap<>(store, (o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Keys cannot be null");
            }
            return o1.compareTo(o2);
        }, new ObjectSerializer(), new ObjectSerializer(), 5, -1);

        for (long i = 1; i < 101; i++) {
            map.put(i, i);
        }

        BaseMap<Long, Long> subMap = (BaseMap<Long, Long>)map.tailMap(5l, false);
        for (long key = 6; key <= subMap.size(); key++) {
            assertEquals(key, (long)subMap.get(key));
        }
        assertNull(subMap.get(5l));
    }

    @Test
    public void testTailMapInclusive() throws IOException {

        final BTreeMap<Long, Long> map = new BTreeMap<>(store, (o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Keys cannot be null");
            }
            return o1.compareTo(o2);
        }, new ObjectSerializer(), new ObjectSerializer(), 5, -1);

        for (long i = 1; i < 101; i++) {
            map.put(i, i);
        }

        BaseMap<Long, Long> subMap = (BaseMap<Long, Long>)map.headMap(5l, true);
        for (long key = 1; key < 6; key++) {
            assertEquals(key, (long)subMap.get(key));
        }
    }
}
