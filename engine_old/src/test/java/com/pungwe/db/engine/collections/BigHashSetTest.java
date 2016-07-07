package com.pungwe.db.engine.collections;

import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.engine.io.store.DirectStore;
import com.pungwe.db.engine.io.store.Store;
import com.pungwe.db.engine.io.volume.HeapByteBufferVolume;
import com.pungwe.db.engine.io.volume.Volume;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by ian on 12/06/2016.
 */
public class BigHashSetTest {

    private Store store;

    @Before
    public void setup() throws Exception {
        Volume volume = new HeapByteBufferVolume("hash", false, 20, -1);
        store = new DirectStore(volume);
    }

    @Test
    public void testAddEntries() throws Exception {

        BigHashSet<String> hashset = new BigHashSet<String>(store, new ObjectSerializer(), 1024);

        for (int i = 0; i < 100; i++) {
            assertTrue(hashset.add("String " + (i + 1)));
        }

        assertTrue(hashset.contains("String 53"));
        assertEquals(100, hashset.size());

    }

    @Test
    public void testDuplicateEntry() throws Exception {
        BigHashSet<String> hashset = new BigHashSet<String>(store, new ObjectSerializer(), 1024);
        assertTrue(hashset.add("my value"));
        assertFalse(hashset.add("my value"));
        assertEquals(1, hashset.size());
    }

    @Test
    public void testIterator() throws Exception {

        List<String> items = new ArrayList<>(100);
        BigHashSet<String> hashset = new BigHashSet<String>(store, new ObjectSerializer(), 1024);
        for (int i = 0; i < 100; i++) {
            assertTrue(hashset.add("String " + (i + 1)));
            items.add("String " + (i + 1));
        }

        int count = 0;
        for (String item : items) {
            for (String stored : hashset) {
                if (item.equals(stored)) {
                    ++count;
                }
            }
        }
        assertEquals(100, count);
    }
}
