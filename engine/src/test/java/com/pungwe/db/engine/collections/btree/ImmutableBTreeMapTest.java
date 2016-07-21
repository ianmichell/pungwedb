/*
 * Copyright (C) 2016 Ian Michell.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pungwe.db.engine.collections.btree;

import com.pungwe.db.core.io.serializers.NumberSerializer;
import com.pungwe.db.core.io.serializers.UUIDSerializer;
import com.pungwe.db.core.utils.UUIDGen;
import com.pungwe.db.engine.io.BasicRecordFile;
import com.pungwe.db.engine.io.RecordFile;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by ian on 11/07/2016.
 */
public class ImmutableBTreeMapTest {

    @Test
    public void writeAndLoad() throws IOException {
        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 100);
        long start = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        File tmp = File.createTempFile("immutable", ".db");
        try {
            RecordFile<AbstractBTreeMap.Node<Long, ?>> recordFile = new BasicRecordFile<>(tmp,
                    ImmutableBTreeMap.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
                            new NumberSerializer<>(Long.class)));
            start = System.nanoTime();
            ImmutableBTreeMap<Long, Long> immutableMap = ImmutableBTreeMap.write(recordFile, "test", map);
            end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to write and load immutable tree", (end - start) / 1000000d));

            System.out.println("File Size in MB: " + ((double)tmp.length() / 1024d / 1024d));
            // Ensure we have every record
            for (long i = 0; i < 10000; i++) {
                assertEquals(new Long(i), immutableMap.get(i));
            }
        } finally {
            tmp.delete();
        }
    }

    @Test
    public void testIterateBackwards() throws Exception {
        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 100);
        long start = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        File tmp = File.createTempFile("immutable", ".db");
        try {
            RecordFile<AbstractBTreeMap.Node<Long, ?>> recordFile = new BasicRecordFile<>(tmp,
                    ImmutableBTreeMap.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
                            new NumberSerializer<>(Long.class)));
            start = System.nanoTime();
            ImmutableBTreeMap<Long, Long> immutableMap = ImmutableBTreeMap.write(recordFile, "test", map);
            end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to write and load immutable tree", (end - start) / 1000000d));

            System.out.println("File Size in MB: " + ((double)tmp.length() / 1024d / 1024d));
            // Ensure we have every record
            AtomicLong counter = new AtomicLong(9999);
            for (Map.Entry<Long, Long> entry : immutableMap.descendingMap().entrySet()) {
                assertEquals(new Long(counter.getAndDecrement()), entry.getKey());
            }
            assertEquals(-1, counter.get());
        } finally {
            tmp.delete();
        }
    }

    @Test
    public void testIterate() throws Exception {
        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 100);
        long start = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        File tmp = File.createTempFile("immutable", ".db");
        try {
            RecordFile<AbstractBTreeMap.Node<Long, ?>> recordFile = new BasicRecordFile<>(tmp,
                    ImmutableBTreeMap.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
                            new NumberSerializer<>(Long.class)));
            start = System.nanoTime();
            ImmutableBTreeMap<Long, Long> immutableMap = ImmutableBTreeMap.write(recordFile, "test", map);
            end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to write and load immutable tree", (end - start) / 1000000d));

            System.out.println("File Size in MB: " + ((double)tmp.length() / 1024d / 1024d));
            // Ensure we have every record
            AtomicLong counter = new AtomicLong(0);
            for (Map.Entry<Long, Long> entry : immutableMap.entrySet()) {
                assertEquals(new Long(counter.getAndIncrement()), entry.getKey());
            }
            assertEquals(10000l, counter.get());
        } finally {
            tmp.delete();
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

        File tmp = File.createTempFile("immutable", ".db");
        try {
            RecordFile<AbstractBTreeMap.Node<Long, ?>> recordFile = new BasicRecordFile<>(tmp,
                    ImmutableBTreeMap.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
                            new NumberSerializer<>(Long.class)));
            start = System.nanoTime();
            ImmutableBTreeMap<Long, Long> immutableMap = ImmutableBTreeMap.write(recordFile, "test", map);
            end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to write and load immutable tree", (end - start) / 1000000d));

            // Iterate a million records
            ConcurrentNavigableMap<Long, Long> sub = immutableMap.subMap(59l, true, 513l, true);
            AtomicLong counter = new AtomicLong(59l);
            Iterator<Map.Entry<Long, Long>> it = sub.entrySet().iterator();
            while (it.hasNext()) {
                assertEquals(new Long(counter.getAndIncrement()), it.next().getKey());
            }
            assertEquals(514l, counter.get());
        } finally {
            tmp.delete();
        }
    }

    @Test
    public void testSubMapIteratorExclusive() throws Exception {

        File tmp = File.createTempFile("immutable", ".db");
        try {
            RecordFile<AbstractBTreeMap.Node<Long, ?>> recordFile = new BasicRecordFile<>(tmp,
                    ImmutableBTreeMap.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
                            new NumberSerializer<>(Long.class)));
            // Add a million records to map
            BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
            long start = System.nanoTime();
            for (long i = 0; i < 10000; i++) {
                assertNotNull(map.put(i, i));
            }
            long end = System.nanoTime();
            long time = end - start;
            System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

            start = System.nanoTime();
            ImmutableBTreeMap<Long, Long> immutableMap = ImmutableBTreeMap.write(recordFile, "test", map);
            end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to write and load immutable tree", (end - start) / 1000000d));

            // Iterate a million records
            ConcurrentNavigableMap<Long, Long> sub = immutableMap.subMap(59l, false, 513l, false);
            AtomicLong counter = new AtomicLong(60);
            Iterator<Map.Entry<Long, Long>> it = sub.entrySet().iterator();
            while (it.hasNext()) {
                assertEquals(new Long(counter.getAndIncrement()), it.next().getKey());
            }
            assertEquals(513l, counter.get());
        } finally {
            tmp.delete();
        }
    }

    @Test
    public void testSubMapIteratorFromExclusive() throws Exception {

        File tmp = File.createTempFile("immutable", ".db");
        try {
            RecordFile<AbstractBTreeMap.Node<Long, ?>> recordFile = new BasicRecordFile<>(tmp,
                    ImmutableBTreeMap.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
                            new NumberSerializer<>(Long.class)));
            // Add a million records to map
            BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
            long start = System.nanoTime();
            for (long i = 0; i < 10000; i++) {
                assertNotNull(map.put(i, i));
            }
            long end = System.nanoTime();
            long time = end - start;
            System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

            start = System.nanoTime();
            ImmutableBTreeMap<Long, Long> immutableMap = ImmutableBTreeMap.write(recordFile, "test", map);
            end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to write and load immutable tree", (end - start) / 1000000d));


            // Iterate a million records
            ConcurrentNavigableMap<Long, Long> sub = immutableMap.subMap(59l, false, 513l, true);
            AtomicLong counter = new AtomicLong(60);
            Iterator<Map.Entry<Long, Long>> it = sub.entrySet().iterator();
            while (it.hasNext()) {
                assertEquals(new Long(counter.getAndIncrement()), it.next().getKey());
            }
            assertEquals(514l, counter.get());
        } finally {
            tmp.delete();
        }
    }

    @Test
    public void testSubMapIteratorToExclusive() throws Exception {

        File tmp = File.createTempFile("immutable", ".db");
        try {
            RecordFile<AbstractBTreeMap.Node<Long, ?>> recordFile = new BasicRecordFile<>(tmp,
                    ImmutableBTreeMap.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
                            new NumberSerializer<>(Long.class)));
            // Add a million records to map
            BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
            long start = System.nanoTime();
            for (long i = 0; i < 10000; i++) {
                assertNotNull(map.put(i, i));
            }
            long end = System.nanoTime();
            long time = end - start;
            System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

            start = System.nanoTime();
            ImmutableBTreeMap<Long, Long> immutableMap = ImmutableBTreeMap.write(recordFile, "test", map);
            end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to write and load immutable tree", (end - start) / 1000000d));


            // Iterate a million records
            ConcurrentNavigableMap<Long, Long> sub = immutableMap.subMap(59l, true, 513l, false);
            AtomicLong counter = new AtomicLong(59);
            Iterator<Map.Entry<Long, Long>> it = sub.entrySet().iterator();
            while (it.hasNext()) {
                assertEquals(new Long(counter.getAndIncrement()), it.next().getKey());
            }
            assertEquals(513l, counter.get());
        } finally {
            tmp.delete();
        }
    }

    //###########################################################################################
    //#                         REVERSE ITERATORS                                               #
    //###########################################################################################

    @Test
    public void testSubMapReverseIterator() throws Exception {

        File tmp = File.createTempFile("immutable", ".db");
        try {
            RecordFile<AbstractBTreeMap.Node<Long, ?>> recordFile = new BasicRecordFile<>(tmp,
                    ImmutableBTreeMap.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
                            new NumberSerializer<>(Long.class)));
            // Add a million records to map
            BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
            long start = System.nanoTime();
            for (long i = 0; i < 10000; i++) {
                assertNotNull(map.put(i, i));
            }
            long end = System.nanoTime();
            long time = end - start;
            System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

            start = System.nanoTime();
            ImmutableBTreeMap<Long, Long> immutableMap = ImmutableBTreeMap.write(recordFile, "test", map);
            end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to write and load immutable tree", (end - start) / 1000000d));

            // Iterate a million records
            ConcurrentNavigableMap<Long, Long> sub = immutableMap.subMap(59l, true, 513l, true);
            AtomicLong counter = new AtomicLong(513);
            Iterator<Map.Entry<Long, Long>> it = sub.descendingMap().entrySet().iterator();
            while (it.hasNext()) {
                assertEquals(new Long(counter.getAndDecrement()), it.next().getKey());
            }
            assertEquals(58, counter.get());
        } finally {
            tmp.delete();
        }
    }

    @Test
    public void testSubMapReverseIteratorExclusive() throws Exception {

        File tmp = File.createTempFile("immutable", ".db");
        try {
            RecordFile<AbstractBTreeMap.Node<Long, ?>> recordFile = new BasicRecordFile<>(tmp,
                    ImmutableBTreeMap.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
                            new NumberSerializer<>(Long.class)));
            // Add a million records to map
            BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
            long start = System.nanoTime();
            for (long i = 0; i < 10000; i++) {
                assertNotNull(map.put(i, i));
            }
            long end = System.nanoTime();
            long time = end - start;
            System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

            start = System.nanoTime();
            ImmutableBTreeMap<Long, Long> immutableMap = ImmutableBTreeMap.write(recordFile, "test", map);
            end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to write and load immutable tree", (end - start) / 1000000d));


            // Iterate a million records
            ConcurrentNavigableMap<Long, Long> sub = immutableMap.subMap(59l, false, 513l, false);
            AtomicLong counter = new AtomicLong(512);
            Iterator<Map.Entry<Long, Long>> it = sub.descendingMap().entrySet().iterator();
            while (it.hasNext()) {
                assertEquals(new Long(counter.getAndDecrement()), it.next().getKey());
            }
            assertEquals(59, counter.get());
        } finally {
            tmp.delete();
        }
    }

    @Test
    public void testSubMapReverseIteratorFromExclusive() throws Exception {

        File tmp = File.createTempFile("immutable", ".db");
        try {
            RecordFile<AbstractBTreeMap.Node<Long, ?>> recordFile = new BasicRecordFile<>(tmp,
                    ImmutableBTreeMap.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
                            new NumberSerializer<>(Long.class)));
            // Add a million records to map
            BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
            long start = System.nanoTime();
            for (long i = 0; i < 10000; i++) {
                assertNotNull(map.put(i, i));
            }
            long end = System.nanoTime();
            long time = end - start;
            System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

            start = System.nanoTime();
            ImmutableBTreeMap<Long, Long> immutableMap = ImmutableBTreeMap.write(recordFile, "test", map);
            end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to write and load immutable tree", (end - start) / 1000000d));


            // Iterate a million records
            ConcurrentNavigableMap<Long, Long> sub = immutableMap.subMap(59l, false, 513l, true);
            AtomicLong counter = new AtomicLong(513);
            Iterator<Map.Entry<Long, Long>> it = sub.descendingMap().entrySet().iterator();
            while (it.hasNext()) {
                assertEquals(new Long(counter.getAndDecrement()), it.next().getKey());
            }
            assertEquals(59, counter.get());
        } finally {
            tmp.delete();
        }
    }

    @Test
    public void testSubMapReverseIteratorToExclusive() throws Exception {

        File tmp = File.createTempFile("immutable", ".db");
        try {
            RecordFile<AbstractBTreeMap.Node<Long, ?>> recordFile = new BasicRecordFile<>(tmp,
                    ImmutableBTreeMap.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
                            new NumberSerializer<>(Long.class)));
            // Add a million records to map
            BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
            long start = System.nanoTime();
            for (long i = 0; i < 10000; i++) {
                assertNotNull(map.put(i, i));
            }
            long end = System.nanoTime();
            long time = end - start;
            System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

            start = System.nanoTime();
            ImmutableBTreeMap<Long, Long> immutableMap = ImmutableBTreeMap.write(recordFile, "test", map);
            end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to write and load immutable tree", (end - start) / 1000000d));

            // Iterate a million records
            ConcurrentNavigableMap<Long, Long> sub = immutableMap.subMap(59l, true, 513l, false);
            AtomicLong counter = new AtomicLong(512);
            Iterator<Map.Entry<Long, Long>> it = sub.descendingMap().entrySet().iterator();
            while (it.hasNext()) {
                assertEquals(new Long(counter.getAndDecrement()), it.next().getKey());
            }
            assertEquals(58, counter.get());
        } finally {
            tmp.delete();
        }
    }


    @Test
    public void testSequentialInsertAndMerge() throws IOException {
        File tmp = File.createTempFile("immutable", ".db");
        try {
            // Create a record file for the combined trees
            RecordFile<AbstractBTreeMap.Node<Long, ?>> keyFile = new BasicRecordFile<>(tmp,
                    ImmutableBTreeMap.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
                            new NumberSerializer<>(Long.class)));

            // Tree 1
            BTreeMap<Long, Long> tree1 = new BTreeMap<>(Long::compareTo, 10);
            for (long i = 0; i < 10000; i++) {
                tree1.put(i, i);
            }
            // Tree 2
            BTreeMap<Long, Long> tree2 = new BTreeMap<>(Long::compareTo, 10);
            for (long i = 10000; i < 20000; i++) {
                tree2.put(i, i);
            }

            long start = System.nanoTime();
            ImmutableBTreeMap<Long, Long> immutableMap = ImmutableBTreeMap.merge(keyFile, null, "test", 100,
                    tree1, tree2, false);
            long end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to merge and load immutable tree", (end - start) / 1000000d));

            System.out.println("File Size in MB: " + ((double)tmp.length() / 1024d / 1024d));
            // Ensure we have every record
            AtomicLong counter = new AtomicLong(0);
            for (Map.Entry<Long, Long> entry : immutableMap.entrySet()) {
                assertEquals(new Long(counter.getAndIncrement()), entry.getKey());
            }
            assertEquals(20000l, counter.get());
        } finally {
            tmp.delete();
        }
    }

    @Test
    public void testSplitInsertAndMerge() throws IOException {
        File tmp = File.createTempFile("immutable", ".db");
        try {
            // Create a record file for the combined trees
            RecordFile<AbstractBTreeMap.Node<Long, ?>> recordFile = new BasicRecordFile<>(tmp,
                    ImmutableBTreeMap.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
                            new NumberSerializer<>(Long.class)));

            // Tree 1
            BTreeMap<Long, Long> tree1 = new BTreeMap<>(Long::compareTo, 10);
            for (long i = 0; i < 20000; i+=2) {
                tree1.put(i, i);
            }
            // Tree 2
            BTreeMap<Long, Long> tree2 = new BTreeMap<>(Long::compareTo, 10);
            for (long i = 1; i < 20000; i+=2) {
                tree2.put(i, i);
            }

            long start = System.nanoTime();
            ImmutableBTreeMap<Long, Long> immutableMap = ImmutableBTreeMap.merge(recordFile, null, "test", 100,
                    tree1, tree2, false);
            long end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to merge and load immutable tree", (end - start) / 1000000d));

            System.out.println("File Size in MB: " + ((double)tmp.length() / 1024d / 1024d));
            // Ensure we have every record
            AtomicLong counter = new AtomicLong(0);
            for (Map.Entry<Long, Long> entry : immutableMap.entrySet()) {
                assertEquals(new Long(counter.getAndIncrement()), entry.getKey());
            }
            assertEquals(20000l, counter.get());
        } finally {
            tmp.delete();
        }
    }

    @Test
    public void testSplitGUIDInsertAndMerge() throws IOException {
        File tmp = File.createTempFile("immutable", ".db");
        try {
            // Create a record file for the combined trees
            RecordFile<AbstractBTreeMap.Node<UUID, ?>> recordFile = new BasicRecordFile<>(tmp,
                    ImmutableBTreeMap.serializer(UUID::compareTo, new UUIDSerializer(), new UUIDSerializer()));

            List<UUID> guids = new ArrayList<>();
            // Tree 1
            BTreeMap<UUID, UUID> tree1 = new BTreeMap<>(UUID::compareTo, 10);
            for (long i = 0; i < 10000; i++) {
                UUID uuid = UUIDGen.getTimeUUID();
                tree1.put(uuid, uuid);
                guids.add(uuid);
            }

            // Tree 2
            BTreeMap<UUID, UUID> tree2 = new BTreeMap<>(UUID::compareTo, 10);
            for (long i = 0; i < 10000; i++) {
                UUID uuid = UUIDGen.getTimeUUID();
                tree2.put(uuid, uuid);
                guids.add(uuid);
            }

            long start = System.nanoTime();
            ImmutableBTreeMap<UUID, UUID> immutableMap = ImmutableBTreeMap.merge(recordFile, null, "test", 100,
                    tree1, tree2, false);
            long end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to merge and load immutable tree", (end - start) / 1000000d));

            System.out.println("File Size in MB: " + ((double)tmp.length() / 1024d / 1024d));

            // Ensure we have every record
            AtomicInteger counter = new AtomicInteger(0);
            for (Map.Entry<UUID, UUID> entry : immutableMap.entrySet()) {
                UUID expect = guids.get(counter.getAndIncrement());
                UUID result = entry.getKey();
                // I will assume these are going to be in order
                assertEquals(expect, result);
            }
            assertEquals(20000l, counter.get());
        } finally {
            tmp.delete();
        }
    }

    @Test
    public void testSplitGUIDInsertAndMergeThreads() throws Exception {
        File tmp = File.createTempFile("immutable", ".db");
        try {
            // Create a record file for the combined trees
            RecordFile<AbstractBTreeMap.Node<UUID, ?>> recordFile = new BasicRecordFile<>(tmp,
                    ImmutableBTreeMap.serializer(UUID::compareTo, new UUIDSerializer(), new UUIDSerializer()));

            // Create an executor service
            ExecutorService executor = Executors.newFixedThreadPool(8);
            final List<Callable<UUID>> threads = new LinkedList<>();
            // Tree 1
            final BTreeMap<UUID, UUID> tree1 = new BTreeMap<>(UUID::compareTo, 10);
            for (long i = 0; i < 10000; i++) {
                threads.add(() -> {
                    UUID uuid = UUIDGen.getTimeUUID();
                    UUID put = tree1.put(uuid, uuid);
                    assertNotNull(put);
                    return put;
                });
            }

            // Tree 2
            final BTreeMap<UUID, UUID> tree2 = new BTreeMap<>(UUID::compareTo, 10);
            for (long i = 0; i < 10000; i++) {
                threads.add(() -> {
                    UUID uuid = UUIDGen.getTimeUUID();
                    UUID put = tree2.put(uuid, uuid);
                    assertNotNull(put);
                    return put;
                });
            }

            List<Future<UUID>> futures = executor.invokeAll(threads);
            List<UUID> guids = new ArrayList<>();

            for (Future<UUID> f : futures) {
                guids.add(f.get());
            }
            guids = guids.stream().sorted(UUID::compareTo).collect(Collectors.toList());

            assertEquals(20000, guids.size());
            assertEquals(guids.get(0), tree1.firstKey());
            assertEquals(10000, tree1.size());
            assertEquals(10000, tree2.size());
            assertEquals(10000, tree1.entrySet().stream().count());
            assertEquals(10000, tree2.entrySet().stream().count());

            long start = System.nanoTime();
            ImmutableBTreeMap<UUID, UUID> immutableMap = ImmutableBTreeMap.merge(recordFile, null,  "test", 100, tree1,
                    tree2, false);
            long end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to merge and load immutable tree", (end - start) / 1000000d));

            System.out.println("File Size in MB: " + ((double)tmp.length() / 1024d / 1024d));

            assertEquals(20000, immutableMap.size());
            assertEquals(guids.get(0), immutableMap.firstKey());

            // Ensure we have every record
            AtomicInteger counter = new AtomicInteger(0);
            for (Map.Entry<UUID, UUID> entry : immutableMap.entrySet()) {
                UUID expect = guids.get(counter.getAndIncrement());
                UUID result = entry.getKey();
                try {
                    assertEquals(expect, result);
                } catch (Throwable t) {
                    System.out.println("Failed at: " + (counter.get() - 1));
                    throw t;
                }
            }
            assertEquals(20000l, counter.get());
        } finally {
            tmp.delete();
        }
    }
}
