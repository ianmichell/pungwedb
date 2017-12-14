package com.pungwe.db.engine.collections;

import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.engine.io.store.DirectStore;
import com.pungwe.db.engine.io.store.Store;
import com.pungwe.db.engine.io.volume.HeapByteBufferVolume;
import com.pungwe.db.engine.io.volume.Volume;
import com.pungwe.db.core.utils.UUIDGen;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by ian on 12/06/2016.
 */
public class BigTreeSetTest {

    private Store store;

    @Before
    public void setup() throws Exception {
        Volume volume = new HeapByteBufferVolume("hash", false, 20, -1);
        store = new DirectStore(volume);
    }

    @Test
    public void testAddEntries() throws Exception {

        BigTreeSet<String> treeSet = new BigTreeSet<String>(store, (o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Keys cannot be null");
            }
            return o1.compareTo(o2);
        }, new ObjectSerializer(), 1024);

        for (int i = 0; i < 100; i++) {
            assertTrue(treeSet.add("String " + (i + 1)));
        }

        assertTrue(treeSet.contains("String 53"));
        assertEquals(100, treeSet.size());

    }

    @Test
    public void testDuplicateEntry() throws Exception {

        BigTreeSet<String> treeSet = new BigTreeSet<String>(store, (o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Keys cannot be null");
            }
            return o1.compareTo(o2);
        },  new ObjectSerializer(), 1024);
        assertTrue(treeSet.add("my value"));
        assertFalse(treeSet.add("my value"));
        assertEquals(1, treeSet.size());
    }

    @Test
    public void testIterator() throws Exception {

        List<UUID> uuids = new ArrayList<UUID>();
        BigTreeSet<UUID> treeSet = new BigTreeSet<UUID>(store, (o1, o2) -> {
            if (o1 == null || o2 == null) {
                throw new IllegalArgumentException("Keys cannot be null");
            }
            return o1.compareTo(o2);
        }, new ObjectSerializer(), 1024);
        for (int i = 0; i < 100; i++) {
            UUID uuid = UUIDGen.getTimeUUID();
            assertTrue(treeSet.add(uuid));
            uuids.add(uuid);
        }

        int count = 0;
        for (UUID item : treeSet) {
            assertEquals(uuids.get(count++), item);
        }
        assertEquals(100, count);
    }

}
