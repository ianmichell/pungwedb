package com.pungwe.db.engine.collections;

import com.pungwe.db.engine.io.store.AppendOnlyStore;
import com.pungwe.db.engine.io.store.DirectStore;
import com.pungwe.db.engine.io.store.Store;
import com.pungwe.db.engine.io.volume.HeapByteBufferVolume;
import com.pungwe.db.engine.io.volume.RandomAccessVolume;
import com.pungwe.db.engine.io.volume.Volume;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by 917903 on 28/06/2016.
 */
public class BLSMTreeMapTest {

    private File tmpFile;
    private AppendOnlyStore store;

    @Before
    public void beforeTest() throws IOException {
        tmpFile = File.createTempFile("lsm_", ".db");
        Volume volume = new RandomAccessVolume("file", tmpFile, false);
        store = new AppendOnlyStore(volume);
    }

    @After
    public void afterTest() throws IOException {
        tmpFile.delete();
    }

    @Test
    public void testPutGet() throws Exception {

        BLSMTreeMap<Long, String> map = new BLSMTreeMap<Long, String>(store, Long::compareTo, 10000, 100);
        for (long i = 0; i < 10; i++) {
            map.put(i + 1l, "Hello World: " + (i + 1));
        }

        assertEquals("Hello World: 7", map.get(7l));
    }
}
