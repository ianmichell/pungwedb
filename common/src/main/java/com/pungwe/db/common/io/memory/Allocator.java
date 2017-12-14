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

import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * Memory Allocator. This interface will provides a template for creating custom allocators based on Native memory (JNA,
 * ByteBuffer, DirectByteBuffer, Memory Mapped File or a plain old Random Access File).
 * <p>
 * Each time memory is allocated a reference is created to track it.
 */
public interface Allocator {


    boolean isEnabled();

    long capacity();

    long remaining();

    /**
     * Attempts to allocate memory of the given size.
     *
     * @param size the amount of memory to acquire in MB.
     *
     * @return the allocated memory or null if memory couldn't be allocated due to lack of available memory.
     *
     * @throws IOException if there was an IO error during allocation.
     */
    AllocatedMemory tryAcquire(long size) throws IOException;

    /**
     * Acquires an AllocatedMemory object of the specified size (if available) and waits until there is some free if
     * none is available...
     *
     * @param size the amount of memory to acquire in MB
     *
     * @throws InterruptedException if the current thread is interrupted whilst waiting.
     * @throws IOException if an IO error occurs during allocation
     *
     * @return allocated memory
     */
    default AllocatedMemory acquire(long size) throws IOException, InterruptedException {
        AllocatedMemory memory = null;
        while ((memory = tryAcquire(size)) == null) {
            Thread.sleep(0, 200);
        }
        return memory;
    }

    /**
     * Acquires an AllocatedMemory object of the specified size (if available) and waits until the timeout has been
     * reached, before throwing a TimeoutException.
     *
     * @param size the amount of memory to acquire.
     * @param timeout the duration of the timeout.
     * @param unit the time unit (nanoseconds, microsends, milliseconds, etc)
     * @throws TimeoutException if the timeout has been reached
     * @throws InterruptedException if the timer has been interrupted.
     * @throws IOException if an IO error occurs during allocation
     *
     * @return allocated memory
     */
    default AllocatedMemory acquire(long size, long timeout, TimeUnit unit) throws IOException, TimeoutException,
            InterruptedException {
        // Try to release memory and loop until the timeout
        long nanoTimeout = unit.toNanos(timeout);
        // Get the minimum wait.. If timeout is less than 200, then we can only wait for that.
        int wait = (int)Math.min(200, nanoTimeout);
        long startNanos = System.nanoTime() + nanoTimeout;
        long endNanos = 0;
        do {
            AllocatedMemory memory = tryAcquire(size);
            if (memory == null) {
                Thread.sleep(0, wait);
                endNanos = System.nanoTime();
                continue;
            }
            return memory;
        } while (startNanos > endNanos);
        throw new TimeoutException("Timeout occurred when attempting to release allocated memory");
    }

    /**
     * Purges all allocated memory, regardless of it being used... It will interrupt any waiting threads. This method
     * will not terminate the running JVM. It is up to the developer to do this when an IOException has been thrown.
     *
     * @throws IOException if there is a problem with purging memory.
     */
    void purgeAll() throws IOException;

    /**
     * Purges the allocated memory, by forcing the write lock to become available and interrupting any threads waiting
     * for it.
     *
     * @param memory the memory to be purged.
     *
     * @throws IOException if a problem occurs (usually if it's already been freed).
     */
    void purge(AllocatedMemory memory) throws IOException;

    /**
     * Attempts to release the allocated memory without waiting, by attempting to acquire a lock on it and then
     * releasing it back to the allocator.
     *
     * @return true if the allocated memory was released. False if it wasn't.
     */
    boolean tryRelease(AllocatedMemory memory) throws IOException;

    /**
     * Release the AllocatedMemory object. If there is a write or read lock on the memory, then this method will wait
     * until it can acquire the write lock.
     *
     * @param memory the allocated memory to be released.
     */
    default void release(AllocatedMemory memory) throws InterruptedException, IOException {
        while (!tryRelease(memory)) {
            Thread.sleep(0, 200);
        }
    }

    /**
     * Releases the AllocatedMemory object. If there is a write or read lock on the memory, then this method will wait
     * until it can can acquire the write lock, or reaches the timeout.
     *
     * @param memory the allocated memory to be released.
     * @param timeout the duration of the timeout.
     * @param unit the unit of time.
     *
     * @throws InterruptedException if the thread was interrupted whilst sleeping.
     * @throws TimeoutException if the timeout is reached.
     */
    default void release(AllocatedMemory memory, long timeout, TimeUnit unit)
            throws InterruptedException, TimeoutException, IOException {
        // Try to release memory and loop until the timeout
        long nanoTimeout = unit.toNanos(timeout);
        // Get the minimum wait.. If timeout is less than 200, then we can only wait for that.
        int wait = (int)Math.min(200, nanoTimeout);
        // The time we have waited
        long timeWaited = 0;
        while (timeWaited < nanoTimeout) {
            if (tryRelease(memory)) {
                return; // If we have released, then return, no need to wait any longer...
            }
            timeWaited = wait;
            Thread.sleep(0, wait);
        }
        throw new TimeoutException("Timeout occurred when attempting to release allocated memory");
    }

    /**
     * Represents a region of allocated region.
     */
    interface AllocatedMemory extends Closeable {

        /**
         * Returns the size of the allocated region of memory.
         *
         * @return the amount of memory allocated in MB
         *
         * @throws IllegalArgumentException if the memory has been freed (closed)
         */
        long getSize();

        /**
         * Has this memory been released?
         *
         * @return true if the memory has been release, false if not.
         */
        boolean isClosed();

        /**
         * Returns the number of remaining bytes. Based on the amount written to memory.
         *
         * @return the number of remaining bytes in the allocated memory.
         *
         * @throws IllegalArgumentException if the memory has been freed (closed)
         */
        long remaining();

        /**
         * DataInput for reading memory
         *
         * @return data input for reading memory
         *
         * @throws IllegalArgumentException if position is greater than the size of the allocated memory, or it has been
         * closed
         */
        default DataInput getDataInput() throws IOException {
            return getDataInput(0);
        }

        /**
         * DataOutput for reading memory
         *
         * @return data output for writing to memory
         *
         * @throws IllegalArgumentException if position is greater than the size of the allocated memory, or it has been
         * closed
         */
        default DataOutput getDataOutput() throws IOException {
            return getDataOutput(0);
        }

        /**
         * DataInput for reading memory
         *
         * @param position the position to set the reader.
         *
         * @return a DataInput object at the position set.
         *
         * @throws IllegalArgumentException if position is greater than the size of the allocated memory, or it has been
         * closed
         */
        DataInput getDataInput(long position) throws IOException;

        /**
         * DataOutput for writing to memory
         *
         * @param position the position to set the writer
         *
         * @return the DataOutput object for writing to memory.
         *
         * @throws IllegalArgumentException if position is greater than the size of the allocated memory, or it has been
         * closed
         */
        DataOutput getDataOutput(long position) throws IOException;

        /**
         * Doesn't attempt to wait for a write lock and simply frees the memory.
         *
         * @throws IOException if there is an error...
         */
        void forceClose() throws IOException;
    }
}
