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
import com.pungwe.db.core.types.DBObject;
import com.pungwe.db.core.utils.UUIDGen;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

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
        Map<String, Object> config = new LinkedHashMap<>();
        Map<String, Boolean> fields = new LinkedHashMap<>();
        fields.put("_id", false);
        config.put("fields", fields);
        config.put("primary", true);
        LSMTreeIndex index = LSMTreeIndex.getInstance(directory, "test-primary", config);
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
        Map<String, Object> config = new LinkedHashMap<>();
        Map<String, Boolean> fields = new LinkedHashMap<>();
        fields.put("_id", false);
        config.put("fields", fields);
        config.put("primary", true);
        LSMTreeIndex index = LSMTreeIndex.getInstance(directory, "test-primary", config);
        DBObject value = new DBObject();
        value.setId(Arrays.asList(1, 2, 3, 4));
        value.put("field", "value");
        index.put(value, 0);
    }

    @Test
    public void secondarySingleKeyUnique() throws Exception {
        File directory = Files.createTempDir();
        Map<String, Object> config = new LinkedHashMap<>();
        Map<String, Boolean> fields = new LinkedHashMap<>();
        fields.put("field", false);
        config.put("fields", fields);
        config.put("primary", false);
        config.put("unique", true);
        LSMTreeIndex index = LSMTreeIndex.getInstance(directory, "test-primary", config);
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
        Map<String, Object> config = new LinkedHashMap<>();
        Map<String, Boolean> fields = new LinkedHashMap<>();
        fields.put("field", false);
        config.put("fields", fields);
        config.put("primary", false);
        config.put("unique", true);
        LSMTreeIndex index = LSMTreeIndex.getInstance(directory, "test-primary", config);
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
        Map<String, Object> config = new LinkedHashMap<>();
        Map<String, Boolean> fields = new LinkedHashMap<>();
        fields.put("field", false);
        config.put("fields", fields);
        config.put("primary", false);
        config.put("unique", true);
        LSMTreeIndex index = LSMTreeIndex.getInstance(directory, "test-primary", config);
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
        Map<String, Object> config = new LinkedHashMap<>();
        Map<String, Boolean> fields = new LinkedHashMap<>();
        fields.put("key", false);
        fields.put("another", false);
        config.put("fields", fields);
        config.put("primary", false);
        config.put("unique", true);
        LSMTreeIndex index = LSMTreeIndex.getInstance(directory, "test-primary", config);
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
    public void secondaryMultiKeyUniqueSingleFieldQuery() throws Exception {
        File directory = Files.createTempDir();
        Map<String, Object> config = new LinkedHashMap<>();
        Map<String, Boolean> fields = new LinkedHashMap<>();
        fields.put("key", false);
        fields.put("another", false);
        config.put("fields", fields);
        config.put("primary", false);
        config.put("unique", true);
        LSMTreeIndex index = LSMTreeIndex.getInstance(directory, "test-primary", config);
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
}
