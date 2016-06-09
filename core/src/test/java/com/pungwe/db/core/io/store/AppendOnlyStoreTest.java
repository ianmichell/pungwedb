package com.pungwe.db.core.io.store;

import com.pungwe.db.core.io.volume.ByteBufferVolume;
import com.pungwe.db.core.io.volume.HeapByteBufferVolume;
import com.pungwe.db.core.io.store.AppendOnlyStore;
import com.pungwe.db.core.utils.Constants;
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

        // Create a new copy of the store
        this.store = new AppendOnlyStore(volume);

        Iterator<Object> it = this.store.iterator();
        assertTrue(it.hasNext());
        assertEquals("Test Record", it.next());
    }
}