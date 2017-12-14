package com.pungwe.db.engine.collections.queue;

import com.google.common.io.Files;
import com.pungwe.db.core.collections.queue.Queue;
import com.pungwe.db.core.io.serializers.StringSerializer;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Created by 917903 on 22/07/2016.
 */
public class BTreeLogQueueTest {

    @Test
    public void testPutPoll() throws Exception {
        File directory = Files.createTempDir();
        try {
            BTreeLogQueue<String> BTreeLogQueue = new BTreeLogQueue<>(directory, "myqueue",
                    new StringSerializer(), 1000, 0);
            // Put a message on the queue
            BTreeLogQueue.put(BTreeLogQueue.newMessage("Hello World"));

            // Pull the message off the queue
            Queue.Message<String> result = BTreeLogQueue.poll(1, TimeUnit.SECONDS);
            assertNotNull(result);
            assertEquals("Hello World", result.getBody());
            assertEquals(Queue.MessageState.PICKED, result.getMessageState());
        } finally {
            directory.delete();
        }
    }

    @Test
    public void testPutPeek() throws Exception {
        File directory = Files.createTempDir();
        try {
            BTreeLogQueue<String> BTreeLogQueue = new BTreeLogQueue<>(directory, "myqueue",
                    new StringSerializer(), 1000, 0);
            // Put a message on the queue
            BTreeLogQueue.put(BTreeLogQueue.newMessage("Hello World"));

            // Pull the message off the queue
            Queue.Message<String> result = BTreeLogQueue.peek(30, TimeUnit.SECONDS);

            assertEquals("Hello World", result.getBody());
            assertTrue(result.isPending());
        } finally {
            directory.delete();
        }
    }

    @Test
    public void testPutFlushPoll() throws Exception {
        File directory = Files.createTempDir();
        try {
            BTreeLogQueue<String> BTreeLogQueue = new BTreeLogQueue<>(directory, "myqueue",
                    new StringSerializer(), 1000, 0);
            long start = System.nanoTime();
            // Put a message on the queue
            for (int i = 0; i < 100000; i++) {
                BTreeLogQueue.put(BTreeLogQueue.newMessage("Hello World: " + i));
            }
            long end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to add to queue", (end - start) / 1000000d));

            long time = 0;
            for (int i = 0; i < 100000; i++) {
                // Pull the message off the queue
                start = System.nanoTime();
                Queue.Message<String> result = BTreeLogQueue.poll(30, TimeUnit.SECONDS);
                end = System.nanoTime();
                long total = (end - start);
                if (total > time) {
                    time = total;
                }
                assertNotNull(result);
                assertEquals("Hello World: " + i, result.getBody());
                assertEquals(Queue.MessageState.PICKED, result.getMessageState());
            }
            System.out.println(String.format("Was: %f ms for the longest", time / 1000000d));
        } finally {
            directory.delete();
        }
    }
}
