/*
 * Copyright (C) 2016 Ian Michell.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pungwe.db.engine.collections.queue;

import com.google.common.io.Files;
import com.pungwe.db.core.collections.queue.Queue;
import com.pungwe.db.core.concurrent.Promise;
import com.pungwe.db.core.io.serializers.StringSerializer;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by ian on 24/07/2016.
 */
public class LogStructuredQueueTest {

    @Test
    public void testPutPoll() throws Exception {
        File directory = Files.createTempDir();
        try {
            LogStructuredQueue<String> logStructuredQueue = new LogStructuredQueue<>("myqueue", directory,
                    new StringSerializer(), 1000, 0);
            // Put a message on the queue
            logStructuredQueue.put(logStructuredQueue.newMessage("Hello World"));
            // Pull the message off the queue
            Queue.Message<String> result = logStructuredQueue.poll();

            assertEquals("Hello World", result.getBody());
            assertEquals(Queue.MessageState.PICKED, result.getMessageState());
        } finally {
            directory.delete();
        }
    }

    @Test
    public void testPromise() throws Exception {
        File directory = Files.createTempDir();
        try {
            LogStructuredQueue<String> logStructuredQueue = new LogStructuredQueue<>("myqueue", directory,
                    new StringSerializer(), 1000, 0);
            // Create a new message called hello world
            Queue.Message<String> put = logStructuredQueue.newMessage("Hello World");
            // Put a message on the queue with a promise.
            Promise<Queue.MessageEvent<String>> promise = logStructuredQueue.putAndPromise(put)
                    .when(e -> e.getState().equals(Queue.MessageState.ACKNOWLEDGED))
                    .promise();
            // Get the message off the queue.
            Queue.Message<String> message = logStructuredQueue.poll(30, TimeUnit.SECONDS);
            // Ensure the id is correct
            assertEquals(put.getId(), message.getId());
            // Acknowledge the message
            message.acknowledge();
            // Get the promise result
            Queue.MessageEvent<String> event = promise.get(30, TimeUnit.SECONDS);
            // Check that it's got the same id
            assertEquals(put.getId(), event.getMessageId());
            // Check that it's got the same status.
            assertEquals(Queue.MessageState.ACKNOWLEDGED, event.getState());
        } finally {
            directory.delete();
        }
    }

    @Test
    public void testPutFlushPoll() throws Exception {
        File directory = Files.createTempDir();
        try {
            LogStructuredQueue<String> logStructuredQueue = new LogStructuredQueue<>("myqueue", directory,
                    new StringSerializer(), 1000, 0);
            long start = System.nanoTime();
            // Put a message on the queue
            for (int i = 0; i < 100000; i++) {
                logStructuredQueue.put(logStructuredQueue.newMessage("Hello World: " + i));
            }
            long end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to add to queue", (end - start) / 1000000d));

            for (int i = 0; i < 100000; i++) {
                // Pull the message off the queue
                try {
                    Queue.Message<String> result = logStructuredQueue.poll(30, TimeUnit.SECONDS);
                    assertNotNull(result);
                    assertEquals("Hello World: " + i, result.getBody());
                    assertEquals(Queue.MessageState.PICKED, result.getMessageState());
                } catch (Throwable t) {
                    System.out.println("Failed at position: " + i);
                    throw t;
                }
            }
        } finally {
            directory.delete();
        }
    }
}
