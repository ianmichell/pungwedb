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
package com.pungwe.db.common.io.memory;

import com.pungwe.db.core.concurrent.Promise;
import com.pungwe.db.core.io.serializers.UUIDSerializer;
import com.pungwe.db.core.utils.ConfigSingleton;
import com.pungwe.db.core.utils.TypeReference;
import com.pungwe.db.core.utils.UUIDGen;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import org.junit.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.*;

/**
 * Created by ian on 09/08/2016.
 */
public class NativeMemoryAllocatorTest {

    @BeforeClass
    public static void setupClass() throws Exception {
        Native.setProtected(true);
    }

    @Before
    public void loadConfig() throws Exception {
        try (InputStream in = NativeMemoryAllocatorTest.class.getResourceAsStream("/config-native-memory.yml")) {
            ConfigSingleton.getInstance().load(in);
        }
    }

    @After
    public void unloadConfig() throws Exception {
        NativeMemoryAllocator.getInstance().purgeAll();
    }

    @Test
    public void testAcquire10MB() throws Exception {
        Allocator.AllocatedMemory memory = NativeMemoryAllocator.getInstance().acquire(10 << 20);
        assertNotNull(memory);
        assertFalse(memory.isClosed());
        assertEquals(10 << 20, memory.getSize());
    }

    @Test
    public void testDisabled() throws Exception {
        Constructor<NativeMemoryAllocator> constructor = NativeMemoryAllocator.class.getDeclaredConstructor(long.class);
        constructor.setAccessible(true);
        NativeMemoryAllocator instance = constructor.newInstance(0L);
        assertFalse(instance.isEnabled());
    }

    @Test(expected = TimeoutException.class)
    public void testAllocateAndTimeout() throws Exception {
        NativeMemoryAllocator.getInstance().acquire(100 << 20);
        NativeMemoryAllocator.getInstance().acquire(100 << 20, 10, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testAllocateAndWait() throws Exception {
        NativeMemoryAllocator.getInstance().purgeAll();
        Allocator.AllocatedMemory memory = NativeMemoryAllocator.getInstance().acquire(100 << 20);
        Promise<Allocator.AllocatedMemory> p = Promise.build(new TypeReference<Allocator.AllocatedMemory>() {})
                .given(() -> NativeMemoryAllocator.getInstance().acquire(100 << 20, 10, TimeUnit.SECONDS)).promise();
        NativeMemoryAllocator.getInstance().release(memory);
        Allocator.AllocatedMemory result = p.get();
        assertEquals(100 << 20, result.getSize());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAllocateTooBig() throws Exception {
        NativeMemoryAllocator.getInstance().acquire(200 << 20);
    }

    @Test
    public void testTryAllocateMultiple() throws Exception {
        // Try to acquire when this thread holds the allocation lock...
        Field field = NativeMemoryAllocator.class.getDeclaredField("allocationLock");
        field.setAccessible(true);
        ReentrantLock lock = ((ReentrantLock)field.get(NativeMemoryAllocator.getInstance()));
        lock.lock();
        try {
            Allocator.AllocatedMemory memory = Promise.build(new TypeReference<Allocator.AllocatedMemory>() {})
                    .given(() -> NativeMemoryAllocator.getInstance().tryAcquire(1 << 20)).promise().get();
            assertNull(memory);
        } finally {
            lock.unlock();
        }
    }

    @Test
    public void testWriteThenRead() throws Exception {
        Allocator.AllocatedMemory memory = NativeMemoryAllocator.getInstance().acquire(100 << 20);
        DataOutput out = memory.getDataOutput();
        UUIDSerializer serializer = new UUIDSerializer();
        for (int i = 0; i < 100000; i++) {
            serializer.serialize(out, UUIDGen.getTimeUUID());
        }
        DataInput in = memory.getDataInput();
        for (int i = 0; i < 100000; i++) {
            UUID id = serializer.deserialize(in);
            assertNotNull(id);
        }
    }

    @Test(expected = IOException.class)
    public void testGetOutputClosed() throws Exception {
        Allocator.AllocatedMemory memory = NativeMemoryAllocator.getInstance().acquire(100 << 20);
        memory.close();
        memory.getDataOutput();
    }

    @Test(expected = IOException.class)
    public void testGetInputClosed() throws Exception {
        Allocator.AllocatedMemory memory = NativeMemoryAllocator.getInstance().acquire(100 << 20);
        memory.close();
        memory.getDataInput();
    }

    @Test(expected = IOException.class)
    public void testGetOutputWriteClosed() throws Exception {
        Allocator.AllocatedMemory memory = NativeMemoryAllocator.getInstance().acquire(100 << 20);
        DataOutput output = memory.getDataOutput();
        memory.close();
        output.write(new byte[100]);
    }

    @Test(expected = IOException.class)
    public void testGetInputReadClosed() throws Exception {
        Allocator.AllocatedMemory memory = NativeMemoryAllocator.getInstance().acquire(100 << 20);
        DataInput input = memory.getDataInput();
        memory.close();
        input.readFully(new byte[100]);
    }

    @Test(expected = IOException.class)
    public void testGetOutputWriteInvalid() throws Exception {
        Allocator.AllocatedMemory memory = NativeMemoryAllocator.getInstance().acquire(100 << 20);
        DataOutput output = memory.getDataOutput();
        output.write(new byte[100], 100, 10);
    }

    @Test(expected = IOException.class)
    public void testGetInputReadInvalid() throws Exception {
        Allocator.AllocatedMemory memory = NativeMemoryAllocator.getInstance().acquire(100 << 20);
        DataInput input = memory.getDataInput();
        input.readFully(new byte[100], 100, 10);
    }
}
