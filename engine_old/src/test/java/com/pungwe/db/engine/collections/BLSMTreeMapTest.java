package com.pungwe.db.engine.collections;

import com.pungwe.db.engine.io.store.BufferedRecordLogStore;
import com.pungwe.db.engine.io.store.CachingStore;
import com.pungwe.db.engine.io.store.Store;
import com.pungwe.db.engine.io.volume.RandomAccessFileVolume;
import com.pungwe.db.engine.io.volume.Volume;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by 917903 on 28/06/2016.
 */
public class BLSMTreeMapTest {

    private File tmpFile;
    private Store store;

    @Before
    public void beforeTest() throws IOException {
        tmpFile = File.createTempFile("lsm_", ".db");
        Volume volume = new RandomAccessFileVolume("file", tmpFile, false);
        store = new CachingStore(new BufferedRecordLogStore(volume, 256 << 20, -1), 10000);
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
        BLSMTreeMap<Long, Long> map = new BLSMTreeMap<>(store, Long::compareTo, 1000, 100);
        List<Callable<Boolean>> threads = new LinkedList<>();
        try {
            for (int i = 0; i < 10000; i++) {
                final long key = i;
                threads.add(() -> map.put(key, key) != null);
            }
            // Timeout after 1 minute...
            long start = System.nanoTime();
            List<Future<Boolean>> futures = executor.invokeAll(threads, 5, TimeUnit.MINUTES);
            long end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000000d));

            // Ensure that we have written every single record....
            for (Future<Boolean> future : futures) {
                try {
                    assert future.get();
                } catch (CancellationException ex) {
                    ex.printStackTrace();
                    throw ex;
                }
            }

            System.out.println("Tree Size: " + store.size());
            for (int i = 0; i < 10000; i++) {
                try {
                    Long get = map.get((long) i);
                    assertNotNull(get);
                    assertEquals(new Long(i), get);
                } catch (Throwable ex) {
                    System.out.println("Failed at record: " + i);
                    ex.printStackTrace();
                    throw ex;
                }
            }
        } finally {
            executor.shutdown();
        }
    }
}
