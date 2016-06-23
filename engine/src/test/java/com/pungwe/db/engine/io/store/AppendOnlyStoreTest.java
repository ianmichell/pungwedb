package com.pungwe.db.engine.io.store;

import com.pungwe.db.engine.io.volume.ByteBufferVolume;
import com.pungwe.db.engine.io.volume.HeapByteBufferVolume;
import com.pungwe.db.engine.utils.Constants;
import com.pungwe.db.core.io.serializers.ObjectSerializer;
import java.io.*;
import java.util.*;
import org.junit.Test;
import org.junit.Before;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class AppendOnlyStoreTest {

    protected ByteBufferVolume volume;
    protected AppendOnlyStore store;

    @Before
    public void setup() throws Exception {
        this.volume = new HeapByteBufferVolume("memory", false, Constants.MIN_PAGE_SHIFT, -1l);
        this.store = new AppendOnlyStore(volume);
    }

    @Test
    public void testAddAppendsHeaderToEnd() throws Exception {

        long position = store.add("My String", new ObjectSerializer());

        DataInput input = this.volume.getDataInput(volume.length());
    }

    @Test
    public void testUpdateAlwaysAppends() throws Exception {

        long position = store.add("My String", new ObjectSerializer());
        long newPosition = store.update(position, "My String", new ObjectSerializer());

        assertNotEquals(position, newPosition);
    }

    @Test
    public void testFindHeader() throws Exception {

        long position = this.store.add("Test Record", new ObjectSerializer());
        this.store.commit();

        // Create a new copy of the store
        Store newStore = new AppendOnlyStore(volume);

        Iterator<Object> it = newStore.iterator();
        assertTrue(it.hasNext());
        assertEquals("Test Record", it.next());
    }

    @Test
    public void testFindHeaderMultiRecord() throws Exception {

        for (int i = 0; i < 100; i++) {
            long position = this.store.add("Test Record: " + (i + 1), new ObjectSerializer());
        }
        this.store.commit();

        // Create a new copy of the store
        Store newStore = new AppendOnlyStore(volume);

        Iterator<Object> it = newStore.iterator();
        assertTrue(it.hasNext());
        assertEquals("Test Record: 1", it.next());
    }

    @Test
    public void testIterator() throws Exception {
        for (int i = 0; i < 100; i++) {
            this.store.add("my string " + (i + 1), new ObjectSerializer());
        }

        this.store.commit();

        int i = 0;
        for (Object value : this.store) {
            assertEquals(("my string " + ++i), (String)value);
        }
        assertEquals(100, i);
    }

    @Test
    public void testRollback() throws Exception {

        this.store.add("My Record", new ObjectSerializer());

        assertEquals(1, this.store.size());

        this.store.rollback();

        assertEquals(0, this.store.size());
    }
}