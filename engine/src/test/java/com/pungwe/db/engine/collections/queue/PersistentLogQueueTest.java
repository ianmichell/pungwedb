package com.pungwe.db.engine.collections.queue;

import com.google.common.io.Files;
import com.pungwe.db.core.collections.queue.Queue;
import com.pungwe.db.core.io.serializers.StringSerializer;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Created by 917903 on 22/07/2016.
 */
public class PersistentLogQueueTest {

    @Test
    public void testPutPoll() throws Exception {
        File directory = Files.createTempDir();
        try {
            PersistentLogQueue<String> persistentLogQueue = new PersistentLogQueue<>(directory, "myqueue",
                    new StringSerializer(), 1000, 0);
            // Put a message on the queue
            persistentLogQueue.put(persistentLogQueue.newMessage("Hello World"));

            // Pull the message off the queue
            Queue.Message<String> result = persistentLogQueue.poll();

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
            PersistentLogQueue<String> persistentLogQueue = new PersistentLogQueue<>(directory, "myqueue",
                    new StringSerializer(), 1000, 0);
            // Put a message on the queue
            persistentLogQueue.put(persistentLogQueue.newMessage("Hello World"));

            // Pull the message off the queue
            Queue.Message<String> result = persistentLogQueue.peek();

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
            PersistentLogQueue<String> persistentLogQueue = new PersistentLogQueue<>(directory, "myqueue",
                    new StringSerializer(), 1000, 0);
            long start = System.nanoTime();
            // Put a message on the queue
            for (int i = 0; i < 10000; i++) {
                persistentLogQueue.put(persistentLogQueue.newMessage("Hello World: " + i));
            }
            long end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to add to queue", (end - start) / 1000000d));

            for (int i = 0; i < 10000; i++) {
                // Pull the message off the queue
                Queue.Message<String> result = persistentLogQueue.pollNoBlock();

                assertEquals("Hello World: " + i, result.getBody());
                assertEquals(Queue.MessageState.PICKED, result.getMessageState());
            }
        } finally {
            directory.delete();
        }
    }
}
