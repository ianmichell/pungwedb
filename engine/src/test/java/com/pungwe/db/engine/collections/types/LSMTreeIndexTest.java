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
package com.pungwe.db.engine.collections.types;

import com.google.common.io.Files;
import com.pungwe.db.core.types.Bucket;
import com.pungwe.db.core.types.DBObject;
import com.pungwe.db.core.utils.UUIDGen;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by ian on 29/07/2016.
 */
public class LSMTreeIndexTest {

    @Test
    public void singleValuePrimary() throws Exception {
        File directory = Files.createTempDir();
        directory.deleteOnExit();
        Bucket.IndexConfig config = new Bucket.IndexConfig("test-primary", true, true, 10000, Collections.singletonList(
                new Bucket.IndexField("_id", false)
        ));
        LSMTreeIndex index = LSMTreeIndex.getInstance(directory, config);
        DBObject value = new DBObject();
        value.setId(UUIDGen.getTimeUUID());
        value.put("field", "value");
        index.put(value, 0);
        assertNotNull(index.get(new DBObject("_id", value.getId())));
        assertEquals(0L, index.get(new DBObject("_id", value.getId())).getValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void singleValuePrimaryArray() throws Exception {
        File directory = Files.createTempDir();
        directory.deleteOnExit();
        Bucket.IndexConfig config = new Bucket.IndexConfig("test-primary", true, true, 10000, Collections.singletonList(
                new Bucket.IndexField("_id", false)
        ));
        LSMTreeIndex index = LSMTreeIndex.getInstance(directory, config);
        DBObject value = new DBObject();
        value.setId(Arrays.asList(1, 2, 3, 4));
        value.put("field", "value");
        index.put(value, 0);
    }

    @Test
    public void secondarySingleKeyUnique() throws Exception {
        File directory = Files.createTempDir();
        directory.deleteOnExit();
        Bucket.IndexConfig config = new Bucket.IndexConfig("test-secondary", false, true, 10000,
                Collections.singletonList(new Bucket.IndexField("field", false)));
        LSMTreeIndex index = LSMTreeIndex.getInstance(directory, config);
        DBObject value = new DBObject();
        value.setId(UUIDGen.getTimeUUID());
        value.put("field", "value");
        index.put(value, 0);
        assertNotNull(index.get(new DBObject("field", "value")));
        assertEquals(value.getId(), index.get(new DBObject("field", "value")).getValue());
    }

    @Test
    public void secondaryArrayKeyUnique() throws Exception {
        File directory = Files.createTempDir();
        directory.deleteOnExit();
        Bucket.IndexConfig config = new Bucket.IndexConfig("test-secondary", false, true, 10000,
                Collections.singletonList(new Bucket.IndexField("field", false)));
        LSMTreeIndex index = LSMTreeIndex.getInstance(directory, config);
        DBObject value = new DBObject();
        value.setId(UUIDGen.getTimeUUID());
        value.put("field", Arrays.asList(1, 2, 3, 4));
        index.put(value, 0);
        assertNotNull(index.get(new DBObject("field", 1)));
        assertEquals(value.getId(), index.get(new DBObject("field", 1)).getValue());
        assertNotNull(index.get(new DBObject("field", 2)));
        assertEquals(value.getId(), index.get(new DBObject("field", 2)).getValue());
        assertNotNull(index.get(new DBObject("field", 3)));
        assertEquals(value.getId(), index.get(new DBObject("field", 3)).getValue());
        assertNotNull(index.get(new DBObject("field", 4)));
        assertEquals(value.getId(), index.get(new DBObject("field", 4)).getValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void secondaryArrayKeyUniqueDuplicate() throws Exception {
        File directory = Files.createTempDir();
        directory.deleteOnExit();
        Bucket.IndexConfig config = new Bucket.IndexConfig("test-primary", false, true, 10000,
                Collections.singletonList(new Bucket.IndexField("field", false)));
        LSMTreeIndex index = LSMTreeIndex.getInstance(directory, config);
        DBObject value = new DBObject();
        value.setId(UUIDGen.getTimeUUID());
        value.put("field", Arrays.asList(1, 2, 3, 4));
        DBObject value2 = new DBObject();
        value2.setId(UUIDGen.getTimeUUID());
        value2.put("field", Arrays.asList(1, 4));
        index.put(value, 0);
        index.put(value2, 0);
    }

    @Test
    public void secondaryMultiKeyUnique() throws Exception {
        File directory = Files.createTempDir();
        directory.deleteOnExit();
        Bucket.IndexConfig config = new Bucket.IndexConfig("test-primary", false, true, 10000,
                Arrays.asList(
                        new Bucket.IndexField("key", false),
                        new Bucket.IndexField("another", false)
                ));
        LSMTreeIndex index = LSMTreeIndex.getInstance(directory, config);
        DBObject value = new DBObject();
        value.setId(UUIDGen.getTimeUUID());
        value.put("key", "value");
        value.put("another", "value");
        assertTrue(index.put(value, 0));
        DBObject key = new DBObject();
        key.put("key", "value");
        key.put("another", "value");
        assertNotNull(index.get(key));
        assertEquals(value.getId(), index.get(key).getValue());
    }

    @Test
    public void secondaryMultiKeyNonUnique() throws Exception {
        File directory = Files.createTempDir();
        directory.deleteOnExit();
        Bucket.IndexConfig config = new Bucket.IndexConfig("test-primary", false, false, 10000,
                Arrays.asList(
                        new Bucket.IndexField("key", false),
                        new Bucket.IndexField("another", false)
                ));
        LSMTreeIndex index = LSMTreeIndex.getInstance(directory, config);
        DBObject value = new DBObject();
        long start = System.nanoTime();
        value.setId(UUIDGen.getTimeUUID());
        value.put("key", "value");
        value.put("another", "value");
        assertTrue(index.put(value, 0));
        for (int i = 0; i < 9999; i++) {
            value = new DBObject();
            value.setId(UUIDGen.getTimeUUID());
            value.put("key", "value");
            value.put("another", "value");
            assertTrue(index.put(value, 0));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put 10000", (end - start) / 1000000d));
        DBObject key = new DBObject();
        key.put("key", "value");
        key.put("another", "value");
        start = System.nanoTime();
        assertNotNull(index.get(key));
        end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to fetch 1", (end - start) / 1000000d));
    }

    @Test
    public void testBuildIndex() throws Exception {
        File directory = Files.createTempDir();
        directory.deleteOnExit();
        // Build the configuration for the index...
        Bucket.IndexConfig config = new Bucket.IndexConfig("test-primary", false, false, 100000,
                Arrays.asList(
                        new Bucket.IndexField("key", false),
                        new Bucket.IndexField("another", false)
                ));
        // Create a new linked list for the values so that we can stream build the index
        List<DBObject> values = new LinkedList<>();
        for (int i = 0; i < 1000; i++) {
            DBObject value = new DBObject();
            value.setId(UUIDGen.getTimeUUID());
            value.put("key", "value");
            value.put("another", "value");
            values.add(value);
        }
        long start = System.nanoTime();
        LSMTreeIndex index = LSMTreeIndex.build(directory, config, values.stream());
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put 10000", (end - start) / 1000000d));
        DBObject key = new DBObject();
        key.put("key", "value");
        key.put("another", "value");
        start = System.nanoTime();
        assertNotNull(index.get(key));
        assertEquals(values.get(0).getId(), index.get(key).getValue());
        end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to fetch 1", (end - start) / 1000000d));
    }

    @Test
    public void secondaryMultiKeyUniqueSingleFieldQuery() throws Exception {
        File directory = Files.createTempDir();
        directory.deleteOnExit();
        Bucket.IndexConfig config = new Bucket.IndexConfig("test-primary", false, true, 10000,
                Arrays.asList(
                        new Bucket.IndexField("key", false),
                        new Bucket.IndexField("another", false)
                ));
        LSMTreeIndex index = LSMTreeIndex.getInstance(directory, config);
        DBObject value = new DBObject();
        value.setId(UUIDGen.getTimeUUID());
        value.put("key", "value");
        value.put("another", "value");
        assertTrue(index.put(value, 0));
        assertNotNull(index.get(new DBObject("key", "value")));
        assertEquals(value.getId(), index.get(new DBObject("key", "value")).getValue());
        assertNotNull(index.get(new DBObject("another", "value")));
        assertEquals(value.getId(), index.get(new DBObject("another", "value")).getValue());
    }

    @Test
    public void testIterator() throws IOException {

    }
}
