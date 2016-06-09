package com.pungwe.db.core.io.store;

import com.pungwe.db.core.io.volume.ByteBufferVolume;
import com.pungwe.db.core.io.volume.HeapByteBufferVolume;
import com.pungwe.db.core.io.store.DirectStore;
import com.pungwe.db.core.utils.Constants;
import com.pungwe.db.core.io.serializers.ObjectSerializer;
import java.io.*;
import java.util.*;
import org.junit.Test;
import org.junit.Before;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DirectStoreTest {

    protected ByteBufferVolume volume;
    protected DirectStore store;

    @Before
    public void setup() throws Exception {
        this.volume = new HeapByteBufferVolume("memory", false, Constants.MIN_PAGE_SHIFT, -1l);
        this.store = new DirectStore(volume);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCreate() throws Exception {
        // Create basic hashmap record
        Map<String, String> record = new HashMap<String, String>();
        record.put("key", "value");

        // Serialize the object into a byte stream for good measure.
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bytesOut);
        new ObjectSerializer().serialize(output, record);

        // Used for to compare the size of what's written
        int size = bytesOut.toByteArray().length;

        // Save the record to the volume and record the offset pointer.
        long pointer = store.add(record, new ObjectSerializer());
        
        // Assert that the pointer is at the end of the first block.
        assertEquals(Constants.BLOCK_SIZE, pointer);

        DataInput input = volume.getDataInput(pointer);

        // Check that the first byte is a record 'R'
        assertEquals((byte)'R', input.readByte());

        // Check that the next 4 bytes are the length and that it's equal to size
        int recordLength = input.readInt();
        assertEquals(size, recordLength);

        // Get next and previous
        long previousRecord = input.readLong();
        assertEquals(-1l, previousRecord);

        long nextRecord = input.readLong();
        assertEquals(Constants.BLOCK_SIZE * 2, nextRecord);

        // Assert that you can deserialize the value
        Map<String, String> recordedValue = (Map<String, String>)new ObjectSerializer().deserialize(input);
        assertEquals("value", recordedValue.get("key"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateAndRead() throws Exception {

        // Create basic hashmap record
        Map<String, String> record = new HashMap<String, String>();
        record.put("key", "value");

        long position = this.store.add(record, new ObjectSerializer());

        assertEquals(1, store.size());

        // Read the value back
        Map<String, String> result = (Map<String, String>)this.store.get(position, new ObjectSerializer());

        assertEquals(record.get("key"), result.get("key"));
    }

    @Test
    public void testCreateAndUpdateInPlace() throws Exception {

        // Create basic hashmap record
        Map<String, String> record = new HashMap<String, String>();
        record.put("key", "value");

        long position = this.store.add(record, new ObjectSerializer());

        assertEquals(1, store.size());

        // Ensure we are using a new hashmap
        record = new HashMap<String, String>();
        record.put("key", "updated value");
        record.put("new_key", "new value");

        long newPosition = this.store.update(position, record, new ObjectSerializer());

        assertEquals(newPosition, position);

        Map<String, String> result = (Map<String, String>)this.store.get(position, new ObjectSerializer());

        assertEquals("updated value", result.get("key"));
        assertEquals("new value", result.get("new_key"));

    }

    @Test
    public void testCreateAndUpdateAndMove() throws Exception {
        int[] test = new int[1024];
        // Pad the test with incremented numbers
        for (int i = 0; i < 1024; i++) {
            test[i] = i;
        }

        int[] update = new int[4096];
        for (int i = 0; i < 4096; i++) {
            update[i] = i;
        }        
        long originalPosition = this.store.add(test, new ObjectSerializer());
        long newPosition = this.store.update(originalPosition, update, new ObjectSerializer());

        assertNotEquals(originalPosition, newPosition);

        // We can't just believe the results from the store. We need to check the volume
        DataInput input = this.volume.getDataInput(originalPosition);
        assertEquals((byte)'M', input.readByte());
        input.skipBytes(12); // skip the size and previous and go straight to the next
        assertEquals(newPosition, input.readLong());
    }

    @Test
    public void testIterator() throws Exception {
        for (int i = 0; i < 100; i++) {
            this.store.add("my string " + (i + 1), new ObjectSerializer());
        }

        int i = 0;
        for (Object value : this.store) {
            assertEquals(("my string " + ++i), (String)value);
        }
        assertEquals(100, i);
    }
}