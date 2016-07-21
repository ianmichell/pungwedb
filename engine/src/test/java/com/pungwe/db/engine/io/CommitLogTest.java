package com.pungwe.db.engine.io;

import com.pungwe.db.core.io.serializers.ByteSerializer;
import com.pungwe.db.core.io.serializers.NumberSerializer;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Created by 917903 on 20/07/2016.
 */
public class CommitLogTest {

    @Test
    public void testWriteInsertAndIterate() throws IOException {
        File tmp = File.createTempFile("commitlog", ".bin");
        try {
            CommitLog<Long> commitLog = new CommitLog<>(tmp, new NumberSerializer<>(Long.class));
            long start = System.nanoTime();
            for (long i = 0; i < 1000; i++) {
                commitLog.append(CommitLog.OP.INSERT, i);
            }
            long end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to write commit log", (end - start) / 1000000d));

            Iterator<CommitLog.Entry<Long>> it = commitLog.iterator();
            AtomicInteger counter = new AtomicInteger();
            CommitLog.Entry<Long> previous = null;
            while (it.hasNext()) {
                CommitLog.Entry<Long> current = it.next();
                assertEquals(CommitLog.OP.INSERT, current.getOp());
                if (previous != null && current.getTimestamp() == previous.getTimestamp()) {
                    assertNotEquals(previous.getInterval(), current.getInterval());
                } else {
                    assertEquals(0, current.getInterval());
                }
                assertEquals(new Long(counter.getAndIncrement()), current.getValue());
                previous = current;
            }
            assertEquals(1000, counter.longValue());
        } finally {
            tmp.delete();
        }
    }

    @Test
    public void testWriteUpdateAndIterate() throws IOException {
        File tmp = File.createTempFile("commitlog", ".bin");
        try {
            CommitLog<Long> commitLog = new CommitLog<>(tmp, new NumberSerializer<>(Long.class));
            long start = System.nanoTime();
            for (long i = 0; i < 1000; i++) {
                commitLog.append(CommitLog.OP.UPDATE, i);
            }
            long end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to write commit log", (end - start) / 1000000d));

            Iterator<CommitLog.Entry<Long>> it = commitLog.iterator();
            AtomicInteger counter = new AtomicInteger();
            CommitLog.Entry<Long> previous = null;
            while (it.hasNext()) {
                CommitLog.Entry<Long> current = it.next();
                assertEquals(CommitLog.OP.UPDATE, current.getOp());
                if (previous != null && current.getTimestamp() == previous.getTimestamp()) {
                    assertNotEquals(previous.getInterval(), current.getInterval());
                } else {
                    assertEquals(0, current.getInterval());
                }
                assertEquals(new Long(counter.getAndIncrement()), current.getValue());
                previous = current;
            }
            assertEquals(1000, counter.longValue());
        } finally {
            tmp.delete();
        }
    }

    @Test
    public void testWriteDeleteAndIterate() throws IOException {
        File tmp = File.createTempFile("commitlog", ".bin");
        try {
            CommitLog<Long> commitLog = new CommitLog<>(tmp, new NumberSerializer<>(Long.class));
            long start = System.nanoTime();
            for (long i = 0; i < 1000; i++) {
                commitLog.append(CommitLog.OP.DELETE, i);
            }
            long end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to write commit log", (end - start) / 1000000d));

            Iterator<CommitLog.Entry<Long>> it = commitLog.iterator();
            AtomicInteger counter = new AtomicInteger();
            CommitLog.Entry<Long> previous = null;
            while (it.hasNext()) {
                CommitLog.Entry<Long> current = it.next();
                assertEquals(CommitLog.OP.DELETE, current.getOp());
                if (previous != null && current.getTimestamp() == previous.getTimestamp()) {
                    assertNotEquals(previous.getInterval(), current.getInterval());
                } else {
                    assertEquals(0, current.getInterval());
                }
                assertEquals(new Long(counter.getAndIncrement()), current.getValue());
                previous = current;
            }
            assertEquals(1000, counter.longValue());
        } finally {
            tmp.delete();
        }
    }

    @Test
    public void testMultiThreaded() throws Exception {
        File tmp = File.createTempFile("commitlog", ".bin");
        ExecutorService executor = Executors.newWorkStealingPool();
        try {
//            CommitLog<Long> commitLog = new CommitLog<>(tmp, new NumberSerializer<>(Long.class));
            CommitLog<byte[]> commitLog = new CommitLog<>(tmp, new ByteSerializer());
            List<Callable<CommitLog.OP>> callables = new LinkedList<>();

            for (long i = 0; i < 10000; i++) {
                final long entry = i;
                callables.add(() -> {
                    int random = ThreadLocalRandom.current().nextInt(0, 2);
                    byte[] bytes = new byte[100];
                    ThreadLocalRandom.current().nextBytes(bytes);
                    commitLog.append(CommitLog.OP.values()[random], bytes);
                    return CommitLog.OP.values()[random];
                });
            }
            long start = System.nanoTime();
            List<Future<CommitLog.OP>> futures = executor.invokeAll(callables);
            long end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to write commit log", (end - start) / 1000000d));

            Iterator<CommitLog.Entry<byte[]>> it = commitLog.iterator();
            AtomicInteger counter = new AtomicInteger();
            CommitLog.Entry<byte[]> previous = null;
            while (it.hasNext()) {
                CommitLog.Entry<byte[]> current = it.next();
                if (previous != null && current.getTimestamp() == previous.getTimestamp()) {
                    assertNotEquals(previous.getInterval(), current.getInterval());
                } else {
                    assertEquals(0, current.getInterval());
                }
                counter.incrementAndGet();
                previous = current;
            }
            assertEquals(10000, counter.longValue());
        } finally {
            tmp.delete();
        }
    }
}
