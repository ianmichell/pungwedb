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

import com.pungwe.db.common.io.AbstractDataInput;
import com.pungwe.db.common.io.AbstractDataOutput;
import com.pungwe.db.core.utils.ConfigSingleton;
import com.sun.jna.Memory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ian on 10/08/2016.
 */
public class HeapMemoryAllocator implements Allocator {

    private static HeapMemoryAllocator INSTANCE;

    private final ReentrantLock allocationLock = new ReentrantLock();
    private final long capacity;
    private final AtomicLong used = new AtomicLong();
    private final Map<HeapAllocatedMemory, WeakReference<HeapAllocatedMemory>> references = new LinkedHashMap<>();

    public HeapMemoryAllocator(long capacity) {
        this.capacity = capacity << 20;
    }

    public static HeapMemoryAllocator getInstance() {
        if (INSTANCE == null) {
            Number c = (Number) ConfigSingleton.getInstance().getByPath("memory.heap.capacity");
            INSTANCE = new HeapMemoryAllocator(c == null ? 0 : c.longValue());
        }
        return INSTANCE;
    }

    @Override
    public boolean isEnabled() {
        return capacity > 0;
    }

    @Override
    public long capacity() {
        return capacity;
    }

    @Override
    public long remaining() {
        return capacity - used.get();
    }

    @Override
    public AllocatedMemory tryAcquire(long size) throws IOException {
        if (!allocationLock.tryLock()) {
            return null;
        }
        try {
            if (size > remaining()) {
                return null;
            }
            if (size > Integer.MAX_VALUE - 1) {
                throw new IllegalArgumentException("You cannot acquire memory greater than 2GB");
            }
            ByteBuffer buffer = ByteBuffer.allocate((int)size);
            HeapAllocatedMemory memory = new HeapAllocatedMemory(buffer);
            references.put(memory, new WeakReference<>(memory));
            return memory;
        } finally {
            if (allocationLock.isHeldByCurrentThread()) {
                allocationLock.unlock();
            }
        }
    }

    @Override
    public void purgeAll() throws IOException {
        // do nothing...
        for (Map.Entry<HeapAllocatedMemory, WeakReference<HeapAllocatedMemory>> memory : references.entrySet()) {
            long size = memory.getKey().getSize();
            memory.getKey().forceClose();
            memory.getValue().clear();
            used.getAndAdd(-size);
        }
    }

    @Override
    public void purge(AllocatedMemory memory) throws IOException {
        long size = memory.getSize();
        memory.forceClose();
        used.getAndAdd(-size);
    }

    @Override
    public boolean tryRelease(AllocatedMemory memory) throws IOException {
        if (!allocationLock.tryLock()) {
            return false;
        }
        try {
            // It's already closed. So just repeat that it is...
            if (memory.isClosed()) {
                return true;
            }
            long size = memory.getSize();
            memory.close();
            if (memory.isClosed()) {
                WeakReference reference = references.remove(memory);
                if (reference != null) {
                    reference.clear();
                }
                used.getAndAdd(-size);
            }
            return memory.isClosed();
        } finally {
            if (allocationLock.isHeldByCurrentThread()) {
                allocationLock.unlock();
            }
        }
    }

    private static class HeapAllocatedMemory implements AllocatedMemory {

        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private final ByteBuffer buffer;
        private final AtomicBoolean closed = new AtomicBoolean();

        public HeapAllocatedMemory(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public long getSize() {
            return buffer.limit();
        }

        @Override
        public boolean isClosed() {
            return closed.get();
        }

        @Override
        public long remaining() {
            return buffer.remaining();
        }

        @Override
        public DataInput getDataInput(long position) {
            if (position > getSize()) {
                throw new IllegalArgumentException("Offset cannot be greater than capacity");
            }
            return new ByteBufferDataInput(this, position);
        }

        @Override
        public DataOutput getDataOutput(long position) {
            if (position > getSize()) {
                throw new IllegalArgumentException("Offset cannot be greater than capacity");
            }
            return new ByteBufferDataOutput(this, position);
        }

        @Override
        public void forceClose() throws IOException {
            closed.set(true);
        }

        @Override
        public void close() throws IOException {
            lock.writeLock().lock();
            try {
                closed.set(true);
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    private static class ByteBufferDataInput extends AbstractDataInput {

        private final ByteBuffer buffer;
        private final HeapAllocatedMemory memory;

        public ByteBufferDataInput(HeapAllocatedMemory memory, long position) {
            super(position);
            this.memory = memory;
            this.buffer = memory.buffer.asReadOnlyBuffer();
            this.buffer.position((int)getPosition());
        }

        @Override
        public void readFully(byte[] b, int off, int len) throws IOException {
            int size = len - off;
            if (size < 0) {
                throw new IllegalArgumentException("You cannot have an offset greater than length");
            }
            memory.lock.readLock().lock();
            try {
                buffer.get(b, off, len);
                position.getAndAdd(size);
            } catch (Throwable t) {
                throw new IOException("Could not read from buffer", t);
            } finally {
                memory.lock.readLock().unlock();
            }
        }
    }

    private static class ByteBufferDataOutput extends AbstractDataOutput {

        private final ByteBuffer buffer;
        private final HeapAllocatedMemory memory;

        public ByteBufferDataOutput(HeapAllocatedMemory memory, long position) {
            super(position);
            this.memory = memory;
            this.buffer = memory.buffer.duplicate();
            this.buffer.position((int)position);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            int size = len - off;
            if (size < 0) {
                throw new IllegalArgumentException("You cannot have an offset greater than length");
            }
            memory.lock.writeLock().lock();
            try {
                buffer.put(b, off, len);
            } catch (Throwable t) {
                throw new IOException("Could not write to buffer", t);
            } finally {
                memory.lock.writeLock().unlock();
            }
        }
    }
}
