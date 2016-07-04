package com.pungwe.db.engine.collections;

import com.pungwe.db.engine.io.store.AppendOnlyStore;
import com.pungwe.db.engine.io.store.CachingStore;
import com.pungwe.db.engine.io.store.Store;
import com.pungwe.db.engine.io.volume.MemoryMappedVolume;
import com.pungwe.db.engine.io.volume.RandomAccessVolume;
import com.pungwe.db.engine.io.volume.Volume;
import com.pungwe.db.engine.utils.Constants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Documented;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Created by 917903 on 28/06/2016.
 */
public class BLSMTreeMapTest {

    private File tmpFile;
    private Store store;

    @Before
    public void beforeTest() throws IOException {
        tmpFile = File.createTempFile("lsm_", ".db");
        Volume volume = new RandomAccessVolume("file", tmpFile, false);
//        store = new CachingStore(new AppendOnlyStore(volume), 10000);
        store = new AppendOnlyStore(new MemoryMappedVolume("file", tmpFile, false, 30));
    }

    @After
    public void afterTest() throws IOException {
        tmpFile.delete();
    }

    @Test
    public void testPutGet() throws Exception {

        BLSMTreeMap<Long, String> map = new BLSMTreeMap<Long, String>(store, Long::compareTo, 1 << 20, 1024);
        long start = System.nanoTime();
        for (long i = 0; i < 100; i++) {
            map.put(i + 1, "Hello World: " + (i + 1));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        assertEquals("Hello World: 7", map.get(7l));
    }

    @Test
    public void testManyMultiThreaded() throws Exception {
        ExecutorService executor = Executors.newWorkStealingPool();
        BLSMTreeMap<Long, Long> map = new BLSMTreeMap<>(store, Long::compareTo, 100000, 1024);
        List<Callable<Long>> threads = new LinkedList<>();
        try {
            for (int i = 0; i < 3000000; i++) {
                final long key = i;
                threads.add(() -> map.put(key, key));
            }
            // Timeout after 1 minute...
            long start = System.nanoTime();
            executor.invokeAll(threads, 1, TimeUnit.MINUTES);
            long end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000000d));

            System.out.println("Tree Size: " + store.size());
            for (int i = 0; i < 1000000; i++) {
                try {
                    long get = map.get((long) i);
                    assertEquals((long) i, get);
                } catch (Throwable ex) {
                    System.out.println("Failed at record: " + i);
                    throw ex;
                }
            }
        } finally {
            executor.shutdown();
        }
    }
}
