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
package com.pungwe.db.common.io;

import com.pungwe.db.core.io.serializers.ByteSerializer;
import com.pungwe.db.core.io.serializers.StringSerializer;
import com.pungwe.db.core.io.serializers.UUIDSerializer;
import com.pungwe.db.core.utils.UUIDGen;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by ian on 07/08/2016.
 */
public class MemoryStoreTest {

    @Test
    public void testPutGet() throws Exception {
        MemoryStore<String> memoryStore = new MemoryStore<>(new StringSerializer(), 2);
        long idx = memoryStore.put("Hello World");
        assertEquals("Hello World", memoryStore.get(idx));
    }

    @Test
    public void testPutGetRemove() throws Exception {
        MemoryStore<String> memoryStore = new MemoryStore<>(new StringSerializer(), 2);
        long idx = memoryStore.put("Hello World");
        assertEquals("Hello World", memoryStore.get(idx));
        long idx2 = memoryStore.put("Hello World 2");
        assertEquals("Hello World 2", memoryStore.get(idx2));
        memoryStore.remove(idx);
        long idx3 = memoryStore.put("Hello World 3");
        assertEquals(idx, idx3);
        assertEquals("Hello World 3", memoryStore.get(idx3));
    }

    @Test
    public void testLargePut() throws Exception {
        MemoryStore<byte[]> memoryStore = new MemoryStore<>(new ByteSerializer(), 3);
        for (int i = 0; i < 10000; i++) {
            memoryStore.put(new byte[100]);
        }
        for (int i = 0; i < 10000; i++) {
            assertNotNull(memoryStore.get(i));
        }
    }
}
