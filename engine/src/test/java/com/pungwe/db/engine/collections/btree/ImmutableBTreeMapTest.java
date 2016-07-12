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
import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.engine.io.BasicRecordFile;
import com.pungwe.db.engine.io.RecordFile;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicLong;

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
                    ImmutableBTree.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
                            new NumberSerializer<>(Long.class)));
            start = System.nanoTime();
            ImmutableBTree<Long, Long> immutableMap = ImmutableBTree.write(recordFile, "test", map);
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
                    ImmutableBTree.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
                            new NumberSerializer<>(Long.class)));
            start = System.nanoTime();
            ImmutableBTree<Long, Long> immutableMap = ImmutableBTree.write(recordFile, "test", map);
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
                    ImmutableBTree.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
                            new NumberSerializer<>(Long.class)));
            start = System.nanoTime();
            ImmutableBTree<Long, Long> immutableMap = ImmutableBTree.write(recordFile, "test", map);
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
                    ImmutableBTree.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
                            new NumberSerializer<>(Long.class)));
            start = System.nanoTime();
            ImmutableBTree<Long, Long> immutableMap = ImmutableBTree.write(recordFile, "test", map);
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
                    ImmutableBTree.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
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
            ImmutableBTree<Long, Long> immutableMap = ImmutableBTree.write(recordFile, "test", map);
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
                    ImmutableBTree.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
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
            ImmutableBTree<Long, Long> immutableMap = ImmutableBTree.write(recordFile, "test", map);
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
                    ImmutableBTree.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
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
            ImmutableBTree<Long, Long> immutableMap = ImmutableBTree.write(recordFile, "test", map);
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
                    ImmutableBTree.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
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
            ImmutableBTree<Long, Long> immutableMap = ImmutableBTree.write(recordFile, "test", map);
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
                    ImmutableBTree.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
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
            ImmutableBTree<Long, Long> immutableMap = ImmutableBTree.write(recordFile, "test", map);
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
                    ImmutableBTree.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
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
            ImmutableBTree<Long, Long> immutableMap = ImmutableBTree.write(recordFile, "test", map);
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
                    ImmutableBTree.serializer(Long::compareTo, new NumberSerializer<>(Long.class),
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
            ImmutableBTree<Long, Long> immutableMap = ImmutableBTree.write(recordFile, "test", map);
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
}
