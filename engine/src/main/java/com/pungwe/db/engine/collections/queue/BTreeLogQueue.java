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

import com.google.common.hash.BloomFilter;
import com.pungwe.db.core.collections.queue.Queue;
import com.pungwe.db.core.concurrent.Promise;
import com.pungwe.db.core.error.DatabaseRuntimeException;
import com.pungwe.db.core.io.serializers.*;
import com.pungwe.db.core.utils.TypeReference;
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
public class BTreeLogQueue<E> implements Queue<E> {

    private static final Logger log = LoggerFactory.getLogger(BTreeLogQueue.class);

    private static final int MAX_PROMISES = 100;
    private static final int MAX_KEYS_PER_NODE = 1024;

    /*
     * We will need to provide some for of locking to ensure when we can flush the nextGeneration index to disk along
     * call the fact when we will be making modifications to each message as they run through the queue.
     */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Collection of listeners for onMessage
     */
    private final List<MessageCallback<E>> listeners = new ArrayList<>();

    /**
     * State change listeners. No useful beyond queue managers.
     */
    private final List<StateChangeEventListener<E>> stateChangeListerners = new ArrayList<>();

    /**
     * Guarantees delivery of a message via a promise, when we need to know something call acknowledged
     */
    private final List<Promise.PromiseBuilder<MessageEvent<E>>> promises = new LinkedList<>();

    private final NavigableMap<UUID, QueueSegment<E>> segments = new ConcurrentSkipListMap<>();
    /**
     * Max retries... If the message fails delivery, it needs to be retried
     */
    private final int maxRetries;
    private final int maxMessagesInMemory;
    private final String name;
    private final File dataDirectory;
    private final Serializer<Message<E>> messageSerializer;
//    private CommitLog<Message<E>> commitLog;
//    private Iterator<Map.Entry<MessageID, Message<E>>> it;

    private Message<E> next;

    public BTreeLogQueue(File dataDirectory, String name, final Serializer<E> valueSerializer,
                         int maxMessagesInMemory, int maxRetries) {
        this.name = name;
        this.dataDirectory = dataDirectory;
        this.maxMessagesInMemory = maxMessagesInMemory;
        this.maxRetries = maxRetries;
        this.messageSerializer = new MessageSerializer<>(valueSerializer);
    }

    private void createNewSegment() throws IOException {
        QueueSegment<E> segment = QueueSegment.newSegment(dataDirectory, name, maxMessagesInMemory,
                messageSerializer);
        segments.put(segment.fileID, segment);
    }

    /**
     * Advances to the next available message.
     */
    private void advance() {
        // Make sure the queue call lock so we have no dodgies...
        lock.readLock().lock();
        try {

            if (segments.isEmpty()) {
                return;
            }
            /*
             * Scan the indexes for a message....
             */
            QueueSegment<E> segment = segments.firstEntry().getValue();
            if (!segment.hasNext()) {
                if (segments.size() == 1) {
                    next = null;
                    return;
                }
                // Purge
                segments.remove(segment.fileID);
                advance();
                return;
            }
            MessageID found = null;
            while (segment.hasNext() && found == null) {
                final MessageID id = segment.next();
                // Does the message exist in any of the segments (from last to first)
                Optional<QueueSegment<E>> existing = segments.descendingMap().tailMap(segment.fileID, false)
                        .entrySet().stream().filter(entry -> entry.getValue().contains(id.id)).map(Map.Entry::getValue)
                        .findFirst();
                if (!existing.isPresent()) {
                    found = id;
                    break;
                }
                if (!existing.get().getState(id.id).equals(MessageState.PENDING)) {
                    continue;
                }
            }
            if (found == null && segments.size() == 1) {
                next = null;
                return;
            } else if (found == null) {
                advance();
            }
            next = segment.get(found.id);
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
        // Ensure next call not null
        if (next == null) {
            advance();
            return;
        }
        lock.readLock().lock();
        try {
            // Add event listeners
            QueueMessage<E> message = buildMessage(next);
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

    private void purgeTreeOldestTree() {

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
        // Check if next call null and try to advance if it call. Then loop until next has a value
        if (next == null) {
            advance();
        }
        /* Whilst next call null, poll every 200 nanos to find a new message */
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
        // If next call still null, then return null
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
        // Ensure the duration call above 0
        if (timeout < 1) {
            throw new IllegalArgumentException("Duration must be greater than 0...");
        }
        // Make sure when nano seconds or microseconds are not set as the time unit...
        if (TimeUnit.NANOSECONDS.equals(timeUnit) || TimeUnit.MICROSECONDS.equals(timeUnit)) {
            throw new IllegalArgumentException("Lowest supported time unit call milliseconds");
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
                // Try to advance when the sleep call done...
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
            if (n == null) {
                return null;
            }
            n.picked();
            advance();
            return n;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param timeout  the amount of time to wait until there call a new message
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

    private QueueMessage buildMessage(final Message<E> from) {
        // Fire the event...
        QueueMessage<E> message = new QueueMessage(next.getId(), next.getHeaders(), next.getBody(), next.getRetries());
        message.onStateChange(this::stateChangeProcessor);
        return message;
    }

    private void stateChangeProcessor(final StateChangeEvent<E> event) {
        Message<E> from = event.getTarget();
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
        // FIXME: Add promise processing... Message changes should be recorded in the commit log
        placeMessageInQueue(replace);
    }

    @Override
    public Promise.PromiseBuilder<MessageEvent<E>> putAndPromise(Message<E> message,
                                                                 Predicate<MessageEvent<E>> predicate)
            throws InterruptedException {
        // Create a promise object for the given message
        Promise.PromiseBuilder<MessageEvent<E>> promise = Promise.build(new TypeReference<MessageEvent<E>>() {})
                .when(e -> e.getMessageId().equals(message.getId())).and(predicate);
        // If promises call greater than MAX_PROMISES
        while (promises.size() >= MAX_PROMISES) {
            Thread.sleep(0, 200);
        }
        // Add the promise to the promise collection
        put(message);
        promises.add(promise);
        return promise;
    }

    @Override
    public Promise.PromiseBuilder<MessageEvent<E>> putAndPromise(Message<E> message,
                                                                 Predicate<MessageEvent<E>> predicate, long timeout,
                                                                 TimeUnit unit)
            throws TimeoutException, InterruptedException {
        // If promises call greater than MAX_PROMISES
        // Ensure the duration call above 0
        if (timeout < 1) {
            throw new IllegalArgumentException("Duration must be greater than 0...");
        }
        // Make sure when nano seconds or microseconds are not set as the time unit...
        if (TimeUnit.NANOSECONDS.equals(unit) || TimeUnit.MICROSECONDS.equals(unit)) {
            throw new IllegalArgumentException("Lowest supported time unit call milliseconds");
        }
        // Create a promise object for the given message
        final Promise.PromiseBuilder<MessageEvent<E>> promise = Promise.build(new TypeReference<MessageEvent<E>>() {})
                .when(e -> e.getMessageId().equals(message.getId())).and(predicate);
        // Calculate what time the timeout will occur.
        long waitFor = System.currentTimeMillis() + unit.toMillis(timeout);
        // Loop, check and wait until the timeout has been reached.
        while (System.currentTimeMillis() < waitFor) {
            if (promises.size() < MAX_PROMISES) {
                // Add the promise to the promise collection
                put(message);
                promises.add(promise);
                return promise;
            }
            Thread.sleep(0, 200);
        }
        throw new TimeoutException("Timed out waiting to put " + message.getId() + " on queue");
    }

    @Override
    public void put(Message<E> message) {
        assert message.isPending() : "Message call not new";
        placeMessageInQueue(message);
    }

    private void placeMessageInQueue(Message<E> message) {
        // Generate a file id
        UUID fileId = UUIDGen.getTimeUUID();
        if (false) {
            try {
                File valueFile = new File(dataDirectory, name + "_" + fileId.toString() + "_data.db");
                RecordFile<Message<E>> values = new BasicRecordFile<>(valueFile, messageSerializer);
            } catch (IOException ex) {
                log.error("Could not create a new record file...");
            }
        }
        if (nextGeneration.size() >= maxMessagesInMemory) {
            // FIXME: Create a factory for record files...
            File keyFile = new File(dataDirectory, name + "_" + fileId.toString() + "_index.db");
            File bloomFile = new File(dataDirectory, name + "_" + fileId.toString() + "_bloom.db");
            Serializer<AbstractBTreeMap.Node<MessageID, ?>> nodeSerializer = ImmutableBTreeMap.serializer(
                    MessageID::compareTo, new MessageIDSerializer(), new NumberSerializer<>(Long.class));
            try {
                RecordFile<AbstractBTreeMap.Node<MessageID, ?>> keys = new BasicRecordFile<>(keyFile, nodeSerializer);
                ImmutableBTreeMap<MessageID, Message<E>> newTree = ImmutableBTreeMap.write(new MessageIDSerializer(),
                        keys, null, bloomFile, name, nextGeneration);
                previousGenerations = Arrays.copyOf(previousGenerations, previousGenerations.length + 1);
                previousGenerations[previousGenerations.length - 1] = newTree;
                // Remove the stale commit log
                this.commitLog.delete();
                // Create a new Tree.
                createNewBTree();
                // Create a new commit log
                this.commitLog = null;
            } catch (IOException ex) {
                log.error("Could not flush the in memory indexes...", ex);
                throw new DatabaseRuntimeException(ex);
            }
        }

        // Add the message into the next gen queue
        try {
            if (nextGeneration.put(new MessageID(message.getId(), message.getMessageState()), message) != null) {
                if (commitLog == null) {
                    File commitLog = new File(dataDirectory, name + "_" + UUIDGen.getTimeUUID().toString()
                            + "_commit.db");
                    this.commitLog = new CommitLog<>(commitLog, messageSerializer);
                }
                // We always insert, offset call 0 as it's in memory
                commitLog.append(CommitLog.OP.INSERT, 0, message);
            }
        } catch (IOException ex) {
            log.error("Could not update commit log!");
        }
        // Fire onMessage listeners if the message call pending...
        if (message.isPending()) {
            fireOnMessage();
        }
    }

    /**
     * Adds an event listener to the queue.
     * <p>
     * Note: There call an array of listeners, adding more than one listener will mean all of them potentially get
     * the same message
     *
     * @param callback the callback to be executed when a new message call available.
     */
    @Override
    public void onMessage(MessageCallback<E> callback) {
        listeners.add(callback);
    }

    private static class QueueMessage<E> extends Message<E> {

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

    private static class MessageSerializer<E> implements Serializer<Message<E>> {

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

    private static class MessageIDSerializer implements Serializer<MessageID> {

        private final UUIDSerializer uuidSerializer = new UUIDSerializer();
        private final boolean ignoreState;

        public MessageIDSerializer() {
            this.ignoreState = false;
        }

        public MessageIDSerializer(boolean ignoreState) {
            this.ignoreState = ignoreState;
        }

        @Override
        public void serialize(DataOutput out, MessageID value) throws IOException {
            uuidSerializer.serialize(out, value.id);
            if (!ignoreState) {
                out.writeByte(value.state.ordinal());
            }
        }

        @Override
        public MessageID deserialize(DataInput in) throws IOException {
            long start = System.nanoTime();
            try {
                UUID id = uuidSerializer.deserialize(in);
                if (!ignoreState) {
                    int state = in.readByte();
                    return new MessageID(id, MessageState.values()[state]);
                }
                return new MessageID(id, MessageState.NONE);
            } finally {
                long end = System.nanoTime();
                if (((end - start) / 1000000d) > 1){
                    System.out.println(String.format("Took: %f ms to deserialize key", (end - start) / 1000000d));
                }
            }
        }

        @Override
        public String getKey() {
            return "PLQ:MSG:ID";
        }
    }

    private static class MessageID implements Comparable<MessageID> {
        private final UUID id;
        private final MessageState state;

        public MessageID(UUID id, MessageState state) {
            this.id = id;
            this.state = state;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MessageID messageID = (MessageID) o;
            return id.equals(messageID.id);

        }

        @Override
        public int compareTo(MessageID o) {
            if (o == null) {
                return 1;
            }
            return id.compareTo(o.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }


    }

    /**
     * Used to store a segment of the queue. This is an append only queue, so when a segment is full, it's tree is
     * pushed to the disk and then a new segment is created... This is created in the style of a LSM...
     *
     * @param <E>
     */
    private static class QueueSegment<E> implements Iterator<MessageID> {

        private final UUID fileID;
        private final File directory;
        private final String name;
        private RecordFile<Message<E>> data;
        private CommitLog<BTreeCommitEntry> commitLog;
        private AbstractBTreeMap<MessageID, Long> index;
        private final int maxSize;
        private MessageID nextPosition;

        private QueueSegment(File directory, String name, UUID fileID, RecordFile<Message<E>> data,
                             CommitLog<BTreeCommitEntry> commitLog, AbstractBTreeMap<MessageID, Long> index,
                             int maxSize) {
            this.fileID = fileID;
            this.data = data;
            this.commitLog = commitLog;
            this.index = index;
            this.maxSize = maxSize;
            this.name = name;
            this.directory = directory;
        }

        public static <E> QueueSegment<E> newSegment(File dataDirectory, String queueName, int maxSize,
                                                     Serializer<Message<E>> serializer) throws IOException {
            // Create the file id.
            UUID fileId = UUIDGen.getTimeUUID();
            // Create a record file for the message data
            if (dataDirectory.exists() && !dataDirectory.isDirectory()) {
                throw new IOException("Expecting a directory, but was not given one...");
            } else if (!dataDirectory.exists() && dataDirectory.mkdirs()) {
                throw new IOException("Could not create data directory");
            }
            // Record file...
            File dataFile = new File(dataDirectory, queueName + "_" + fileId.toString() + "_data.db");
            RecordFile<Message<E>> data = new BasicRecordFile<>(dataFile, serializer);
            // Index - We do not want more than MAX_KEYS_PER_NODE, but should honour the max size...
            BTreeMap<MessageID, Long> index = new BTreeMap<>(new MessageIDSerializer(), MessageID::compareTo,
                    Math.min(maxSize, MAX_KEYS_PER_NODE));
            // Commit Log...
            File commitLogFile = new File(dataDirectory, queueName + "_" + fileId.toString() + "_commit.db");
            CommitLog<BTreeCommitEntry> commitLog = new CommitLog<>(commitLogFile, new BTreeCommitEntrySerializer());
            // Create new instance
            return new QueueSegment<>(dataDirectory, queueName, fileId, data, commitLog, index, maxSize);
        }

        public void put(Message<E> message) {
            // Checks if this is immutable...
            if (isFull()) {
                throw new UnsupportedOperationException("This segment is immutable or full!");
            }
            try {
                MessageID messageId = new MessageID(message.getId(), message.getMessageState());
                // Add the record to the data file
                long position = data.writer().append(message);
                // Create a commit log entry
                commitLog.append(CommitLog.OP.INSERT, position, new BTreeCommitEntry(
                        message.getId(), message.getMessageState(), position, message.getRetries()));
                // Add to the index.
                if (index.put(messageId, position) == null) {
                    throw new DatabaseRuntimeException("Could not write message to index");
                }

                // If we have reached the limit of this segment, then we need to lock it.
                if (index.size() == maxSize) {
                    Serializer<AbstractBTreeMap.Node<MessageID, ?>> serializer = ImmutableBTreeMap.serializer(
                            MessageID::compareTo, new MessageIDSerializer(), new NumberSerializer<>(Long.class));
                    File file = new File(directory, name + "_" + fileID + "_index.db");
                    File bloomFile = new File(directory, name + "_" + fileID + "_bloom.db");
                    RecordFile<AbstractBTreeMap.Node<MessageID, ?>> keys = new BasicRecordFile<>(file, serializer);
                    index = ImmutableBTreeMap.write(new MessageIDSerializer(true), keys, bloomFile, name, index);
                }
            } catch (IOException ex) {
                throw new DatabaseRuntimeException("Could not save message to disk", ex);
            }
        }

        public Message<E> get(UUID messageId) {
            // Don't need to worry about message state
            Long offset = index.get(new MessageID(messageId, MessageState.PENDING));
            if (offset == null) {
                return null;
            }
            try {
                return data.get(offset);
            } catch (IOException ex) {
                throw new DatabaseRuntimeException(ex);
            }
        }

        public MessageState getState(UUID messageId) {
            Map.Entry<MessageID, Long> entry = index.getEntry(new MessageID(messageId, MessageState.PENDING));
            return entry == null ? null : entry.getKey().state;
        }

        public boolean contains(UUID messageId) {
            return index.containsKey(new MessageID(messageId, MessageState.PENDING));
        }

        public MessageID lower(UUID messageId, Predicate<MessageID> predicate) {
            for (MessageID key : index.descendingKeySet().headSet(new MessageID(messageId, MessageState.PENDING))) {
                if (predicate.test(key)) {
                    return key;
                }
            }
            return null;
        }

        public MessageID higher(UUID messageId, Predicate<MessageID> predicate) {
            for (MessageID key : index.keySet().tailSet(new MessageID(messageId, MessageState.PENDING))) {
                if (predicate.test(key)) {
                    return key;
                }
            }
            return null;
        }

        public boolean isFull() {
            return ImmutableBTreeMap.class.isAssignableFrom(index.getClass()) || index.size() == maxSize;
        }

        public void delete() {

        }

        @Override
        public boolean hasNext() {
            if (nextPosition == null) {
                advance();
            }
            return nextPosition != null;
        }

        @Override
        public MessageID next() {
            if (!hasNext()) {
                return null;
            }
            MessageID next = nextPosition;
            if (next != null) {
                advance();
            }
            return next;
        }

        public void advance() {
            NavigableSet<MessageID> keys = index.keySet();
            if (nextPosition != null) {
                keys = keys.tailSet(nextPosition, true);
            }
            Optional<MessageID> found = keys.stream().filter(m -> m.state.equals(MessageState.PENDING)).findFirst();
            nextPosition = found.orElse(null);
        }

        public void resetPosition() {
            nextPosition = null;
        }
    }

    private static class BTreeCommitEntry {
        private final UUID messageId;
        private final MessageState messageState;
        private final long offset;
        private final int retries;

        public BTreeCommitEntry(UUID messageId, MessageState messageState, long offset, int retries) {
            this.messageId = messageId;
            this.messageState = messageState;
            this.retries = retries;
            this.offset = offset;
        }

        public UUID getMessageId() {
            return messageId;
        }

        public MessageState getMessageState() {
            return messageState;
        }

        public long getOffset() {
            return offset;
        }

        public int getRetries() {
            return retries;
        }
    }

    private static class BTreeCommitEntrySerializer implements Serializer<BTreeCommitEntry> {

        final UUIDSerializer uuidSerializer = new UUIDSerializer();

        @Override
        public void serialize(DataOutput out, BTreeCommitEntry value) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(bytes);
            uuidSerializer.serialize(dataOut, value.getMessageId());
            dataOut.writeByte(value.getMessageState().ordinal());
            dataOut.writeLong(value.getOffset());
            dataOut.writeInt(value.getRetries());
            out.write(bytes.toByteArray());
        }

        @Override
        public BTreeCommitEntry deserialize(DataInput in) throws IOException {
            UUID id = uuidSerializer.deserialize(in);
            byte state = in.readByte();
            long offset = in.readLong();
            int retries = in.readInt();
            return new BTreeCommitEntry(id, MessageState.values()[state], offset, retries);
        }

        @Override
        public String getKey() {
            return "BTCE";
        }
    }
}
