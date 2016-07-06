package com.pungwe.db.engine.io.store;

import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.engine.io.volume.RandomAccessFileVolume;
import com.pungwe.db.engine.io.volume.Volume;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInput;
import java.io.File;
import java.util.Iterator;

import static org.junit.Assert.*;

public class BufferedRecordLogStoreTest {

    protected Volume volume;
    protected BufferedRecordLogStore store;

    @Before
    public void setup() throws Exception {
//        this.volume = new HeapByteBufferVolume("memory", false, 1 << 30, -1l);
        File file = File.createTempFile("append_store", ".db");
        file.deleteOnExit();
        this.volume = new RandomAccessFileVolume(file);
        this.store = new BufferedRecordLogStore(volume, 16 << 20, -1);
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

        this.store.add("Test Record", new ObjectSerializer());
        this.store.commit();

        // Create a new copy of the store
        Store newStore = new BufferedRecordLogStore(volume, 16 << 20, -1);

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
        Store newStore = new BufferedRecordLogStore(volume, 16 << 20, -1);

        Iterator<Object> it = newStore.iterator();
        assertTrue(it.hasNext());
        assertEquals("Test Record: 1", it.next());
    }

    @Test
    public void testIterator() throws Exception {
        for (int i = 0; i < 1000000; i++) {
            this.store.add("my string " + (i + 1), new ObjectSerializer());
        }
        System.out.println("Finished add a million records!");
        this.store.commit();

        assertEquals(1000000, store.size());
        int i = 1;
        System.out.println("Iterating over a million records!");
        for (Object value : this.store) {
            assertEquals(("my string " + i++), (String)value);
        }
        assertEquals(1000000, i);
        System.out.println("Finished iterating over a million records!");
    }

    @Test
    public void testRollback() throws Exception {

        this.store.add("My Record", new ObjectSerializer());

        assertEquals(1, this.store.size());

        this.store.rollback();

        assertEquals(0, this.store.size());
    }
}