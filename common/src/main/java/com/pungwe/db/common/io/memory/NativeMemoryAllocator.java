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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ian on 09/08/2016.
 */
public final class NativeMemoryAllocator implements Allocator {

    private static NativeMemoryAllocator INSTANCE;

    private final ReentrantLock allocationLock = new ReentrantLock();
    private final long capacity;
    private final AtomicLong used = new AtomicLong();
    private final Map<NativeAllocatedMemory, WeakReference<NativeAllocatedMemory>> references = new LinkedHashMap<>();

    private NativeMemoryAllocator(long capacity) {
        this.capacity = capacity << 20;
    }

    public static NativeMemoryAllocator getInstance() {
        if (INSTANCE == null) {
            Number c = (Number)ConfigSingleton.getInstance().getByPath("memory.native.capacity");
            INSTANCE = new NativeMemoryAllocator(c == null ? 0 : c.longValue());
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
        if (size > capacity()) {
            throw new IllegalArgumentException("You cannot allocate more than the maximum capacity of: "
                    + capacity);
        }
        if (!allocationLock.tryLock()) {
            return null;
        }
        try {
            if (remaining() > size) {
                Memory memory = new Memory(size);
                NativeAllocatedMemory allocated = new NativeAllocatedMemory(memory);
                references.put(allocated, new WeakReference<>(allocated));
                // Increase the amount used...
                used.getAndAdd(size);
                // Return the allocated memory...
                return allocated;
            }
            // Return null if we can't get it...
            return null;
            // Chances are... This will never be thrown...
        } catch (Throwable t) {
            throw new IOException("There was a problem allocating memory", t);
        } finally {
            if (allocationLock.isHeldByCurrentThread()) {
                allocationLock.unlock();
            }
        }
    }

    @Override
    public boolean tryRelease(AllocatedMemory memory) throws IOException {
        if (!allocationLock.tryLock()) {
            return false;
        }
        try {
            if (memory.isClosed()) {
                return true;
            }
            long size = memory.getSize();
            memory.close();
            if (memory.isClosed()) {
                WeakReference<NativeAllocatedMemory> reference = references.remove(memory);
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

    @Override
    public void purge(AllocatedMemory memory) throws IOException {
        allocationLock.lock();
        try {
            // Kill the current write lock. Then the read locks...
            long size = memory.getSize();
            memory.forceClose();
            used.getAndAdd(-size);
            if (references.remove(memory) == null) {
                throw new IOException("Could not remove reference to memory");
            }
        } finally {
            allocationLock.unlock();
        }
    }

    @Override
    public void purgeAll() throws IOException {
        allocationLock.lock();
        try {
            for (Map.Entry<NativeAllocatedMemory, WeakReference<NativeAllocatedMemory>> entry : references.entrySet()) {
                long size = entry.getKey().getSize();
                entry.getKey().forceClose();
                entry.getValue().clear();
            }
            references.clear();
            used.set(0);
        } finally {
            allocationLock.unlock();
        }
    }

    private final static class NativeAllocatedMemory implements AllocatedMemory {

        // Set to tue if this object has been freed / closed.
        private final AtomicBoolean closed = new AtomicBoolean();
        // Read write lock to ensure fine grained thread safety.
        final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        // Memory object that contains the reference to the region of memory allocated.
        private Memory memory;
        // Stores the amount of used memory.
        private final AtomicLong used = new AtomicLong();

        public NativeAllocatedMemory(Memory memory) {
            this.memory = memory;
        }

        @Override
        public long getSize() {
            return memory == null ? 0 : memory.size();
        }

        @Override
        public boolean isClosed() {
            return closed.get();
        }

        @Override
        public long remaining() {
            return memory.size() - used.get();
        }

        @Override
        public void close() throws IOException {
            // Free the memory by way of the allocator
            readWriteLock.writeLock().lock();
            try {
                closed.set(true);
                memory = null;
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }

        @Override
        public void forceClose() throws IOException {
            // Foce the close regardless...
            closed.set(true);
            memory = null;
        }

        @Override
        public DataInput getDataInput(long position) throws IOException {
            if (closed.get()) {
                throw new IOException("Allocated memory has been freed");
            }
            return new NativeMemoryDataInput(this, position);
        }

        @Override
        public DataOutput getDataOutput(long position) throws IOException {
            if (closed.get()) {
                throw new IOException("Allocated memory has been freed");
            }
            return new NativeMemoryDataOutput(this, position);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NativeAllocatedMemory that = (NativeAllocatedMemory) o;
            return memory.equals(that.memory);

        }

        @Override
        public int hashCode() {
            return memory == null ? Integer.MIN_VALUE : memory.hashCode();
        }
    }

    private static final class NativeMemoryDataOutput extends AbstractDataOutput {

        private final ReentrantReadWriteLock.WriteLock lock;
        private final NativeAllocatedMemory memory;

        public NativeMemoryDataOutput(NativeAllocatedMemory memory, long position) {
            super(position);
            this.memory = memory;
            this.lock = memory.readWriteLock.writeLock();
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            lock.lock();
            try {
                if (memory.isClosed()) {
                    throw new IOException("Memory has been freed");
                }
                memory.memory.write(position.get(), b, off, len);
            } catch (Throwable t) {
                throw new IOException("Could not data write to memory", t);
            } finally {
                lock.unlock();
            }
        }
    }

    private static final class NativeMemoryDataInput extends AbstractDataInput {

        private final ReentrantReadWriteLock.ReadLock lock;
        private final NativeAllocatedMemory memory;

        private NativeMemoryDataInput(NativeAllocatedMemory memory, long position) {
            super(position);
            this.memory = memory;
            this.lock = memory.readWriteLock.readLock();
        }

        @Override
        public void readFully(byte[] b, int off, int len) throws IOException {
            int read = len - off;
            // Ensure that we have more than one byte of data to be read...
            if (read < 1) {
                throw new IOException("Amount of data to be read is less than 1 byte!");
            }
            // Read the data from memory
            this.lock.lock();
            try {
                // Ensure that the memory isn't freed...
                if (memory.isClosed()) {
                    throw new IOException("Memory has been freed");
                }
                // Read the memory.
                memory.memory.read(position.get(), b, off, len);
                // Update the current position...
                position.getAndAdd(read);
            } catch (Throwable t) {
                throw new IOException("Could not read from memory", t);
            } finally {
                this.lock.unlock();
            }
        }
    }
}
