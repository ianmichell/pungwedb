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

import com.pungwe.db.core.io.serializers.Serializer;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Very similar to <code>RecordFile</code> in that we store serialized records, with one notable difference. First off
 * we store an additional 12 bytes per entry. There is a memory region which contains the peer (ptr) to each allocated
 * block of memory per record.
 * <p>
 * This is used to for memory management purposes... When finalize is called, we will spin through the pointers and free
 * up the memory.
 * <p>
 * The array of pointers is also used for removal... When a record is removed, it's "pointer" is freed up. The next
 * write to the store, will result in that record being reused.
 *
 * @param <E> the type of object being stored in memory...
 */
@Deprecated
public class MemoryStore<E> {

    private static final Logger log = LoggerFactory.getLogger(MemoryStore.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Memory pointers;
    private long[] freed = new long[0];
    private final Serializer<E> serializer;
    private final long capacity;
    private final AtomicLong used;
    private final AtomicLong position;
    private final AtomicLong size;

    /**
     *
     * @param serializer
     * @param capacity in megabytes
     */
    public MemoryStore(Serializer<E> serializer, long capacity) {
        // Values are always serialized into memory...
        this.serializer = serializer;
        // set the capacity
        this.capacity = capacity << 20;
        // Take 10 percent of the space...
        this.pointers = new Memory(1 << 20);
        // Keeps track of the total amount of used memory...
        this.used = new AtomicLong(pointers.size());
        // Position of pointers
        this.position = new AtomicLong(0);
        // Set the size (number of records)
        this.size = new AtomicLong(0);
    }

    /**
     * Writes value to Memory and returns it's pointer...
     *
     * @param value the value to be stored
     *
     * @return the pointer offset...
     *
     * @throws IOException if an error during allocation or writing occurs.
     */
    public long put(E value) throws IOException {
        lock.writeLock().lock();
        try {
            // Check for a free record id in pointers...
            long[] free = findFreePointers();
            long freedPosition = free.length > 0 ? free[0] : -1;
            // Once the pointer region has been increased in size, we can add our value as bytes..
            return freedPosition >= 0 ? writeValue(freedPosition, value) : writeValue(value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long[] put(E... value) throws IOException {
        if (value.length < 1) {
            throw new IllegalArgumentException("There should be at least one value being passed in!");
        }
        return putAll(Arrays.asList(value));
    }

    public long[] putAll(Collection<E> values) throws IOException {
        if (values.size() < 1) {
            throw new IllegalArgumentException("There should be at least one value being passed in!");
        }
        lock.writeLock().lock();
        try {
            if (values.size() < 1) {
                return new long[0];
            }
            // Record ids to return...
            long[] ptrs = new long[values.size()];
            // If we have free pointers, then use them...
            int counter = 0;
            for (E value : values) {
                if (counter < freed.length) {
                    ptrs[counter] = writeValue(freed[counter], value);
                } else {
                    ptrs[counter] = writeValue(value);
                }
            }
            return ptrs;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public E get(long idx) throws IOException {
        long offset = idx * 8;
        lock.readLock().lock();
        try {
            if (offset > position.get()) {
                throw new IndexOutOfBoundsException("No object at index: " + idx + " out of bounds");
            }
            // Get the pointer for the given position...
            long peer = pointers.getLong(offset);
            // Now that we have the pointer, we can fetch the object... Start by retrieving the pointer...
            Pointer ptr = new Pointer(peer);
            if (peer <= 0) {
                throw new IOException("Could not find memory for index: " + idx + " with pointer: " + peer);
            }
            // Get the length of the data...
            int length = ptr.getInt(0);
            // Get the byte array containing the data
            byte[] serialized = ptr.getByteArray(4, length);
            // Create an input stream and read the value with the serializer...
            ByteArrayInputStream bytes = new ByteArrayInputStream(serialized);
            DataInputStream in = new DataInputStream(bytes);
            return serializer.deserialize(in);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void remove(long idx) throws IOException {
        long offset = idx * 8;
        lock.writeLock().lock();
        try {
            if (offset > position.get()) {
                throw new IndexOutOfBoundsException("No object at index: " + idx + " out of bounds");
            }
            // Find the pointer...
            long peer = pointers.getLong(offset);
            // If it's already free. Then don't bother!
            if (peer <= 0) {
                return;
            }
            try {
                // Get a Pointer reference, so we can at least read the size of the value...
                Pointer ptr = new Pointer(peer);
                // Fetch the size of the record from position 0 of the peer, including the size of the int...
                int size = ptr.getInt(0) + 4;
                // Free the memory!
                Native.free(peer);
                // Set freed
                freed = Arrays.copyOf(freed, freed.length + 1);
                freed[freed.length - 1] = offset;
                // Now set pointers array element at idx to -1
                pointers.setLong(offset, -1);
                // Decrement used by the value size of the record...
                used.getAndAdd(-(size));
            } catch (Throwable t) {
                log.error("Could not free memory at: " + peer, t);
                throw new IOException("Could not free memory at: " + peer, t);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     *
     * @return -1 if there are no free pointers, or the offset that's free.
     *
     * @throws IOException
     */
    private long[] findFreePointers() throws IOException {
        return freed;
    }

    private void increasePointerSize() throws IOException {
        checkBounds(pointers.size() + (1 << 20));
        // Create a new memory pointer...
        Memory newPointer = new Memory((1 << 20) + pointers.size());
        // Copy the values of pointers to new pointer
        long read = 0;
        while (read < pointers.size()) {
            byte[] buffer = new byte[1 << 20];
            pointers.read(read, buffer, 0, buffer.length);
            newPointer.write(read, buffer, 0, buffer.length);
            read += buffer.length;
        }
        used.getAndAdd(1 << 20);
        pointers = newPointer;
    }

    private long writeValue(E value) throws IOException {
        if (position.get() >= pointers.size()) {
            increasePointerSize();
        }
        // Write the value to position
        long index = writeValue(position.get(), value);
        // Increase position by size of long
        position.getAndAdd(8);
        // return the index of the pointer!
        return index;
    }

    private long writeValue(long offset, E value) throws IOException {
        // Create a byte array to write the values...
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        // Serialize the value into a byte array...
        serializer.serialize(out, value);
        // get a reference to the byte array from the output stream...
        byte[] serialized = bytes.toByteArray();
        // Check the capacity
        checkBounds(serialized.length + 4);
        // Allocate memory
        long ptr = -1;
        try {
            ptr = Native.malloc(serialized.length + 4);
            if (ptr <= 0) {
                throw new IOException("Could not allocate memory");
            }
            // Get a pointer object
            Pointer p = new Pointer(ptr);
            // Set the value length... Important so we know how much to read back out as an array...
            p.setInt(0, serialized.length);
            // Write the value of serialized to the pointer...
            p.write(4, serialized, 0, serialized.length);
            // Add the address to pointers...
            pointers.setLong(offset, ptr);
            // Increment used by 8 bytes + size of serialized
            used.getAndAdd(serialized.length + 4);
            // Increase size
            size.getAndIncrement();
            // Return the "pointers" offset.
            return offset / 8;
        } catch (Throwable t) {
            // log something, free the memory and throw an IOException
            log.error("Could not allocate memory", t);
            if (ptr > -1) {
                Native.free(ptr);
            }
            throw new IOException("Could not write object to memory", t);
        }
    }

    @SuppressWarnings("FinalizeDoesntCallSuperFinalize")
    @Override
    protected void finalize() throws Throwable {
        // Free all the pointers... the Pointers Memory will free itself...
        for (long i = 0; i < position.get(); i+=8) {
            long ptr = pointers.getLong(i);
            if (ptr > 0) {
                Native.free(ptr);
            }
        }
    }

    // FIXME: Add a custom exception...
    private void checkBounds(long size) throws IOException {
        if ((used.get() + size) > capacity) {
            throw new IOException("Capacity limit has been reached");
        }
    }
}
