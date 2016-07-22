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

import com.pungwe.db.core.collections.queue.Queue;
import com.pungwe.db.core.concurrent.Promise;
import com.pungwe.db.core.error.DatabaseException;
import com.pungwe.db.core.error.DatabaseRuntimeException;
import com.pungwe.db.core.io.serializers.*;
import com.pungwe.db.core.utils.UUIDGen;
import com.pungwe.db.engine.collections.btree.AbstractBTreeMap;
import com.pungwe.db.engine.collections.btree.BTreeMap;
import com.pungwe.db.engine.collections.btree.ImmutableBTreeMap;
import com.pungwe.db.engine.io.BasicRecordFile;
import com.pungwe.db.engine.io.CommitLog;
import com.pungwe.db.engine.io.RecordFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

// FIXME: Needs a bloom filter!

/**
 * @param <E>
 */
public class PersistentLogQueue<E> implements Queue<E> {

    private static final Logger log = LoggerFactory.getLogger(PersistentLogQueue.class);

    private static final int MAX_PROMISES = 1000; // No more than 1000 promises.
    /*
     * We will need to provide some for of locking to ensure when we can flush the nextGeneration index to disk along
     * with the fact when we will be making modifications to each message as they run through the queue.
     */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Memory store is a btree when is used to ensure message order by UUID, track changes and ensure
     * when we don't get repeat messages running through the queue.
     * <p>
     * Messages work from old to to new, so this tree will be where messages are inserted. When it is full it will be
     * pushed to disk, so when data is not lost.
     * <p>
     * It is also managed by a CommitLog when is cycled each time the tree is recreated...
     */
    private BTreeMap<UUID, Message<E>> nextGeneration;

    /**
     * Old generation btree maps. These immutable trees contain the oldest records,
     * with previousGenerations[0] being the oldest. These trees are created when data overflows the
     * nextGeneration tree and needs more permanent storage.
     * <p>
     * When a previous generate file is obsolete, it's purged from the array and it's files deleted.
     */
    @SuppressWarnings("unchecked")
    private ImmutableBTreeMap<UUID, Message<E>>[] previousGenerations = new ImmutableBTreeMap[0];

    /**
     * Collection of listeners for onMessage
     */
    private final List<MessageCallback<E>> listeners = new ArrayList<>();

    /**
     * State change listeners. No useful beyond queue managers.
     */
    private final List<StateChangeEventListener<E>> stateChangeListerners = new ArrayList<>();

    /**
     * Guarantees delivery of a message via a promise, when we need to know something is acknowledged
     */
    private final NavigableMap<UUID, Promise<MessageEvent<E>>> promises = new ConcurrentSkipListMap<>();

    /**
     * Max retries... If the message fails delivery, it needs to be retried
     */
    private final int maxRetries;
    private final int maxMessagesInMemory;
    private final String name;
    private final File dataDirectory;
    private final Serializer<Message<E>> messageSerializer;
    private CommitLog<Message<E>> commitLog;

    private Message<E> next;

    public PersistentLogQueue(File dataDirectory, String name, final Serializer<E> valueSerializer,
                              int maxMessagesInMemory, int maxRetries) {
        this.name = name;
        this.dataDirectory = dataDirectory;
        this.maxMessagesInMemory = maxMessagesInMemory;
        this.maxRetries = maxRetries;
        this.nextGeneration = new BTreeMap<>(new UUIDSerializer(), UUID::compareTo, 100);
        this.messageSerializer = new MessageSerializer(valueSerializer);

    }

    /**
     * Advances to the next available message.
     */
    private void advance() {
        // Make sure the queue is lock so we have no dodgies...
        lock.readLock().lock();
        try {
            /*
             * If next is null and the previousGenerations array is empty, then we simply return the first message
             * we can find from the nextGeneration map.
             */
            if (next == null && previousGenerations.length == 0) {
                for (Map.Entry<UUID, Message<E>> entry : nextGeneration.entrySet()) {
                    if (entry.getValue().isPending()) {
                        next = entry.getValue();
                        return;
                    }
                }
                // If we get here, we might as well return as there is nothing in the nextGeneration.
                return;
            }

            /*
             * Scan the indexes for a message....
             */
            AbstractBTreeMap<UUID, Message<E>> tree = null;
            if (previousGenerations.length > 0) {
                tree = previousGenerations[0];
            } else {
                tree = nextGeneration;
            }

            /*
             * The first tree in the array are the oldest messages in the queue stored, each level we move
             * up through is technically a newer set of messages. On a good day, this loop won't have to run as
             * the older data will be purged from the disk to save space. One would hope (especially with
             * replication queues when the messages are picked up immediately, or within a few hours. Lots of
             * traffic will cause disk flushes and slow disk io down.
             *
             * If the current next element is not null, then we simply want a newer message, so we loop
             * through until we find one, not ideal but with next as a starting point it should be a lot quicker
             *
             */
            Map.Entry<UUID, Message<E>> currentEntry = next == null ? tree.firstEntry() :
                    tree.higherEntry(next.getId());

            // If currentEntry is null, then we are not going to find anything in this tree
            if (currentEntry == null && BTreeMap.class.isAssignableFrom(tree.getClass())) {
                // We're out of entries!
                next = null;
                return;
            } else if (currentEntry == null) {
                // Purge the oldest tree and run advance again..
                purgeTreeOldestTree();
                advance();
                return;
            }

            // Create placeholder for nextEntry
            Map.Entry<UUID, Message<E>> nextEntry = null;
            // Loop whilst we have no next entry.
            while (nextEntry == null) {
                // If the current entry is null, we have run out of tree
                if (currentEntry == null) {
                    purgeTreeOldestTree();
                    advance();
                    return;
                }

                // If the message is not pending, we don't want to faff about, just go to the next one
                if (!currentEntry.getValue().isPending()) {
                    currentEntry = tree.higherEntry(currentEntry.getKey());
                    continue;
                }

                // Is there an existing message? Hopefully not, but if it is
                Message<E> existingMessage = findMessageInTrees(tree, currentEntry.getKey());
                // If the message is null, then yay, we have a pending message
                if (existingMessage == null) {
                    nextEntry = currentEntry;
                    break;
                }

                // Existing message is not null, we need to check the status
                if (existingMessage.isPending()) {
                    // If the message is pending... Then we can set next.
                    nextEntry = currentEntry;
                    break;
                }

                // If we get here, the current entry is not pending... So find the next message and repeat...
                currentEntry = tree.higherEntry(currentEntry.getKey());
            }

            /*
             * We need to search the trees for an older entry just to be when the safe side... This will happen in
             * reverse order, so when one of the newer trees will find it if at all. There should never be billions
             * of records here... But then again profiling will take care of this...
             */
            Message<E> older = findOlderPendingMessageInTrees(tree, currentEntry.getKey());
            if (older != null) {
                next = older;
                fireOnMessage();
                return;
            }

            // We have the next entry... So we can set next.
            next = nextEntry.getValue();
            fireOnMessage();
        } finally {
            lock.readLock().unlock();
        }
    }

    private void fireOnMessage() {
        if (listeners.size() == 0) {
            // If we have no listeners... Don't do anything
            return;
        }
        // Ensure next is not null
        if (next == null) {
            advance();
            return;
        }
        lock.readLock().lock();
        try {
            // Create a new copy of the message
            final Message<E> message = new QueueMessage(next.getId(), next.getHeaders(), next.getBody(), next.getRetries());
            // Add event listeners
            buildMessage(message);
            // Set the message to picked
            message.picked();
            // Fire the onMessage event for the current message
            listeners.parallelStream().forEach(eMessageCallback -> {
                // Fire event
                eMessageCallback.onMessage(message);
            });
            // Once completed advance
            advance();
        } finally {
            lock.readLock().unlock();
        }
    }

    private Message<E> findOlderPendingMessageInTrees(AbstractBTreeMap<UUID, Message<E>> current, UUID id) {

        // If current is the next gen map, then we have the newest available map...
        if (BTreeMap.class.isAssignableFrom(current.getClass())) {
            // If older entry is null, then we don't have an older entry
            return null;
        }

        // Check next gen
        Message<E> message = findOlderPendingMessageInTree(nextGeneration, id);
        // If the message is not null. Then return it, as it's older and pending...
        if (message != null) {
            return message;
        }

        // If we only have one tree in previous generations... Then don't bother as it's already discounted.
        if (previousGenerations.length <= 1) {
            return null;
        }

        // FIXME: This might not need to be a loop, because performance might suck
        // Check the other trees in reverse order and never check the tree at index 0 as it's discounted already
        // There shouldn't be more than depth 2 or 3
        for (int i = previousGenerations.length - 1; i > 0; i--) {
            message = findOlderPendingMessageInTree(previousGenerations[i], id);
            if (message != null) {
                return message;
            }
        }
        return null;
    }

    private Message<E> findOlderPendingMessageInTree(AbstractBTreeMap<UUID, Message<E>> current, UUID id) {
        // Higher entry is newer than the old entry
        Set<Map.Entry<UUID, Message<E>>> entries = current.headMap(id).entrySet();
        // Iterate each entry, until we have
        for (Map.Entry<UUID, Message<E>> entry : entries) {
            // We have an older entry!
            if (entry.getValue().isPending()) {
                return entry.getValue();
            }
        }
        return null;
    }

    private Message<E> findMessageInTrees(AbstractBTreeMap<UUID, Message<E>> current, UUID id) {
        // if current is nextGen, then we don't want to check for the message in there...
        if (BTreeMap.class.isAssignableFrom(current.getClass())) {
            return null;
        }
        // Check every tree, except the bottom tree
        for (int i = previousGenerations.length - 1; i > 0; i--) {
            // Does it exist, well running "get" will return null if it doesn't
            Message<E> message = previousGenerations[i].get(id);
            // If there is one in there, then we need to return it. This should be the newest copy...
            if (message != null) {
                return message;
            }
        }
        return null;
    }

    private void purgeTreeOldestTree() {
        if (previousGenerations.length == 0) {
            return; // do nothing
        }
        ImmutableBTreeMap<UUID, Message<E>> treeToPurge = previousGenerations[0];
        // This tree is finished, we can remove it
        previousGenerations = Arrays.copyOfRange(previousGenerations, 1, previousGenerations.length);
        // Purge the tree from disk...
        treeToPurge.delete();
    }

    @Override
    public Message<E> newMessage(E body) {
        return new QueueMessage(UUIDGen.getTimeUUID(), MessageState.PENDING, new LinkedHashMap<>(), body, 0);
    }

    /**
     * @return
     * @throws InterruptedException
     */
    @Override
    public Message<E> peek() throws InterruptedException {
        // Check if next is null and try to advance if it is. Then loop until next has a value
        if (next == null) {
            advance();
        }
        /* Whilst next is null, poll every 200 nanos to find a new message */
        while (next == null) {
            Thread.sleep(0, 200);
            advance();
        }
        // Return a copy of the message
        Message<E> message = new QueueMessage(next.getId(), next.getHeaders(), next.getBody(), next.getRetries());
        buildMessage(message);
        return message;
    }

    @Override
    public Message<E> peekNoBlock() {
        if (next == null) {
            advance();
        }
        // If next is still null, then return null
        if (next == null) {
            return null;
        }
        Message<E> message = new QueueMessage(next.getId(), next.getHeaders(), next.getBody(), next.getRetries());
        buildMessage(message);
        return message;
    }

    /**
     * @param timeout  the timeout for blocking for a new message
     * @param timeUnit the unit of measure for time (milliseconds, seconds, minutes, days).
     * @return
     * @throws TimeoutException
     * @throws InterruptedException
     */
    @Override
    public Message<E> peek(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        // Ensure the duration is above 0
        if (timeout < 1) {
            throw new IllegalArgumentException("Duration must be greater than 0...");
        }
        // Make sure when nano seconds or microseconds are not set as the time unit...
        if (TimeUnit.NANOSECONDS.equals(timeUnit) || TimeUnit.MICROSECONDS.equals(timeUnit)) {
            throw new IllegalArgumentException("Lowest supported time unit is milliseconds");
        }
        // Calculate what time the timeout will occur.
        long waitFor = System.currentTimeMillis() + timeUnit.toMillis(timeout);
        // Loop, check and wait until the timeout has been reached.
        while (System.currentTimeMillis() < waitFor) {
            lock.readLock().lock();
            try {
                if (next != null) {
                    break;
                }
                Thread.sleep(0, 200);
                // Try to advance when the sleep is done...
                advance();
            } finally {
                lock.readLock().unlock();
            }
        }
        // Return a copy of the message
        Message<E> message = new QueueMessage(next.getId(), next.getHeaders(), next.getBody(), next.getRetries());
        buildMessage(message);
        return message;
    }

    /**
     * @return
     * @throws InterruptedException
     */
    @Override
    public Message<E> poll() throws InterruptedException {
        lock.writeLock().lock();
        try {
            Message<E> n = peek();
            n.picked();
            advance();
            return n;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Message<E> pollNoBlock() {
        lock.writeLock().lock();
        try {
            Message<E> n = peekNoBlock();
            n.picked();
            advance();
            return n;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param timeout  the amount of time to wait until there is a new message
     * @param timeUnit the unit of measure for the timeout.
     * @return
     * @throws TimeoutException
     * @throws InterruptedException
     */
    @Override
    public Message<E> poll(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        lock.writeLock().lock();
        try {
            Message<E> n = peek(timeout, timeUnit);
            // Set the message to picked...
            n.picked();
            advance();
            return n;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void buildMessage(final Message<E> from) {
        // Fire the event...
        from.onStateChange(event -> {
            // Event for when
            QueueMessage replace = new QueueMessage(from.getId(), from.getHeaders(), from.getBody(), from.getRetries());
            switch (event.getTo()) {
                case EXPIRED:
                case FAILED: {
                    if (replace.getRetries() + 1 > maxRetries) {
                        break;
                    }
                    // Retry the message, this won't fire any events, but will change the state to pending and increment
                    // the retry count...
                    replace.retry();
                    break;
                }
                default: {
                    // We don't want to fire anymore events here. Simply update the status
                    replace.setMessageState(event.getTo());
                    break;
                }
            }
            // FIXME: Add promise processing...
            placeMessageInQueue(replace);
        });
    }

    @Override
    public Promise<MessageEvent<E>> putAndPromise(final Message<E> message) throws InterruptedException {
        return putAndPromise(message, eMessageEvent -> eMessageEvent.getState().equals(
                MessageState.ACKNOWLEDGED) && eMessageEvent.getMessageId().equals(message.getId()));
    }

    @Override
    public Promise<MessageEvent<E>> putAndPromise(Message<E> message, long timeout, TimeUnit unit)
            throws TimeoutException, InterruptedException {

        return putAndPromise(message, eMessageEvent -> eMessageEvent.getState().equals(
                MessageState.ACKNOWLEDGED) && eMessageEvent.getMessageId().equals(message.getId()), timeout, unit);
    }

    @Override
    public Promise<MessageEvent<E>> putAndPromise(Message<E> message, Predicate<MessageEvent<E>> predicate)
            throws InterruptedException {
        // Create a promise object for the given message
        Promise<MessageEvent<E>> promise = Promise.when(predicate);
        // If promises is greater than MAX_PROMISES
        while (promises.size() >= MAX_PROMISES) {
            Thread.sleep(0, 200);
        }
        // Add the promise to the promise collection
        put(message);
        return promises.put(message.getId(), promise);
    }

    @Override
    public Promise<MessageEvent<E>> putAndPromise(Message<E> message, Predicate<MessageEvent<E>> predicate,
                                                  long timeout, TimeUnit unit)
            throws TimeoutException, InterruptedException {
        // If promises is greater than MAX_PROMISES
        // Ensure the duration is above 0
        if (timeout < 1) {
            throw new IllegalArgumentException("Duration must be greater than 0...");
        }
        // Make sure when nano seconds or microseconds are not set as the time unit...
        if (TimeUnit.NANOSECONDS.equals(unit) || TimeUnit.MICROSECONDS.equals(unit)) {
            throw new IllegalArgumentException("Lowest supported time unit is milliseconds");
        }
        // Create a promise object for the given message
        final Promise<MessageEvent<E>> promise = Promise.when(predicate);
        // Calculate what time the timeout will occur.
        long waitFor = System.currentTimeMillis() + unit.toMillis(timeout);
        // Loop, check and wait until the timeout has been reached.
        while (System.currentTimeMillis() < waitFor) {
            if (promises.size() < MAX_PROMISES) {
                // Add the promise to the promise collection
                put(message);
                return promises.put(message.getId(), promise);
            }
            Thread.sleep(0, 200);
        }
        throw new TimeoutException("Timed out waiting to put " + message.getId() + " on queue");
    }

    @Override
    public void put(Message<E> message) {
        assert message.isPending() : "Message is not new";
        placeMessageInQueue(message);
    }

    private void placeMessageInQueue(Message<E> message) {
        if (nextGeneration.size() >= maxMessagesInMemory) {
            UUID fileId = UUIDGen.getTimeUUID();
            // FIXME: Create a factory for record files...
            File keyFile = new File(dataDirectory, name + "_" + fileId.toString() + "_index.db");
            File valueFile = new File(dataDirectory, name + "_" + fileId.toString() + "_data.db");
            Serializer<AbstractBTreeMap.Node<UUID, ?>> nodeSerializer = ImmutableBTreeMap.serializer(UUID::compareTo,
                    new UUIDSerializer(), new NumberSerializer<>(Long.class));
            try {
                RecordFile<AbstractBTreeMap.Node<UUID, ?>> keys = new BasicRecordFile<>(keyFile, nodeSerializer);
                RecordFile<Message<E>> values = new BasicRecordFile<>(valueFile, messageSerializer);
                ImmutableBTreeMap<UUID, Message<E>> newTree = ImmutableBTreeMap.write(keys, values, name,
                        nextGeneration);
                previousGenerations = Arrays.copyOf(previousGenerations, previousGenerations.length + 1);
                previousGenerations[previousGenerations.length - 1] = newTree;
                // Remove the stale commit log
                this.commitLog.delete();
                // Create a new Tree.
                nextGeneration = new BTreeMap<>(new UUIDSerializer(), UUID::compareTo, 100);
                // Create a new commit log
                this.commitLog = null;
            } catch (IOException ex) {
                log.error("Could not flush the in memory indexes...", ex);
                throw new DatabaseRuntimeException(ex);
            }
        }

        // Add the message into the nextgen queue
        try {
            if (nextGeneration.put(message.getId(), message) != null) {
                if (commitLog == null) {
                    File commitLog = new File(dataDirectory, name + "_" + UUIDGen.getTimeUUID().toString()
                            + "_commit.db");
                    this.commitLog = new CommitLog<>(commitLog, messageSerializer);
                }
                // We always insert
                commitLog.append(CommitLog.OP.INSERT, message);
            }
        } catch (IOException ex) {
            log.error("Could not update commit log!");
        }
        // Fire onMessage listeners if the message is pending...
        if (message.isPending()) {
            fireOnMessage();
        }
    }

    /**
     * Adds an event listener to the queue.
     * <p>
     * Note: There is an array of listeners, adding more than one listener will mean all of them potentially get
     * the same message
     *
     * @param callback the callback to be executed when a new message is available.
     */
    @Override
    public void onMessage(MessageCallback<E> callback) {
        listeners.add(callback);
    }

    private class QueueMessage extends Message<E> {

        public QueueMessage(UUID id, Map<String, Object> headers, E body, int retries) {
            super(id, headers, body);
            this.retries.set(retries);
        }

        public QueueMessage(UUID id, MessageState state, Map<String, Object> headers, E body, int retries) {
            super(id, headers, body);
            this.setMessageState(state);
            this.retries.set(retries);
        }

        protected void setMessageState(MessageState state) {
            this.messageState = state;
        }
    }

    private class MessageSerializer implements Serializer<Message<E>> {

        private final Serializer<E> valueSerializer;

        public MessageSerializer(Serializer<E> valueSerializer) {
            this.valueSerializer = valueSerializer;
        }

        @Override
        public void serialize(DataOutput out, Message<E> value) throws IOException {
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(bytesOut);
            // Write the ID
            new UUIDSerializer().serialize(dataOut, value.getId());
            // Write the message state
            dataOut.writeByte(value.getMessageState().ordinal());
            // Write the number of retries
            dataOut.writeInt(value.getRetries());
            // Write headers
            new MapSerializer<>(new StringSerializer(), new ObjectSerializer()).serialize(dataOut,
                    value.getHeaders());
            // Write message
            valueSerializer.serialize(dataOut, value.getBody());

            // write output
            out.write(bytesOut.toByteArray());
        }

        @Override
        public Message<E> deserialize(DataInput in) throws IOException {
            UUID id = new UUIDSerializer().deserialize(in);
            int state = in.readByte();
            int retries = in.readInt();
            // Headers
            Map<String, Object> headers = new MapSerializer<>(new StringSerializer(), new ObjectSerializer())
                    .deserialize(in);
            // Body
            E body = valueSerializer.deserialize(in);
            return new QueueMessage(id, MessageState.values()[state], headers, body, retries);
        }

        @Override
        public String getKey() {
            return "QM:" + valueSerializer.getKey();
        }
    }
}
