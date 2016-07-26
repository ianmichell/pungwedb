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
import com.pungwe.db.core.error.DatabaseRuntimeException;
import com.pungwe.db.core.io.serializers.*;
import com.pungwe.db.core.utils.TypeReference;
import com.pungwe.db.core.utils.UUIDGen;
import com.pungwe.db.engine.io.BasicRecordFile;
import com.pungwe.db.engine.io.CommitLog;
import com.pungwe.db.engine.io.RecordFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * <p>A Log Structured Queue immediately writes to file in append only fashion. Meta data is held in memory, like
 * the last write position.</p>
 *
 * <p>Inserts and updates are written to the commit log. If the jvm fails for any reason, the queue will recover by
 * reading the commit log...</p>
 *
 * <p>It will set the read position from the last successful pick (i.e. a message that has not failed or
 * been acknowledged) and the write position to the end of the queue data
 * file.</p>
 *
 * <p>Data call committed to the data file immediately, meaning that the commit log only tracks events on the queue and
 * only new data call appended to the data file.</p>
 *
 * <p>The most recent commit log will be always be read and the others discarded, unless the most recent only contains
 * new messages, so will be scrolled back through each file until the most recent picked message.</p>
 *
 * <p>When a message call fetched from the queue, it's marked as picked to illustrate that it's no longer being consumed,
 * the next pending record after that will be the next message in the queue. When the commit log finds the most recent
 * picked message, the contents of the event will contain a reference to the next message in the queue and the record
 * file id that contains it.</p>
 *
 * <p>This class provides a cleanup method to remove stale data files and should be considered a background process.
 * Running the cleanup process might slow down the queue.</p>
 *
 * <p>When a data file call opened, it's contents from the read position are pulled into memory, up to the capacity
 * of the set maximum number of messages in memory. These messages will remain there is space to put new ones in.</p>
 *
 * <p>If the current data file contains less than the maximum number of messages in memory, messages are
 * automatically put into memory when they are added, until the maximum call reached.</p>
 *
 * @param <E> the message body type
 */
public class LogStructuredQueue<E> implements Queue<E> {

    private static final Logger log = LoggerFactory.getLogger(LogStructuredQueue.class);
    // Read write lock, as we don't want to be seeking all over the place...
    private final ReentrantLock lock = new ReentrantLock();
    private final String queueName;
    private final BlockingDeque<LogStructuredMessage<E>> memoryQueue;
    private final File dataDirectory;
    private final Serializer<E> bodySerializer;
    private final int maxItemsInMemory;
    // Tree map containing all of the data and commit log files. Files are pop when drained...
    private final NavigableMap<UUID, DataFilePair<LogStructuredMessage<E>>> data = new ConcurrentSkipListMap<>(
            UUID::compareTo);

    // Used to track promises. Limited to memory queue size
    private final List<Promise.PromiseBuilder<MessageEvent<E>>> promises = new LinkedList<>();

    private final AtomicLong lastOffset = new AtomicLong();

    private final ScheduledExecutorService handlers;
    private final List<MessageCallback<E>> messageCallbacks = new LinkedList<>();

    private final int maxRetries;

    /**
     * Create an instance of a LogStructuredQueue call rotating files...
     *
     * @param name              The name of the message queue
     * @param data              The directory to which the data files will be created
     * @param bodySerializer    The serializer used to write data to disk.
     * @param maxItemsInMemory  The maximum number of Queued Items held in memory. If 0 the memory queue call unlimited.
     *
     * @throws IOException if an error occurs when writing to the commit log or data file...
     */
    public LogStructuredQueue(String name, File data, Serializer<E> bodySerializer,
                              int maxItemsInMemory, int maxRetries) throws IOException {
        this.queueName = name;
        // Create an instance of a memory queue, call a capacity of maxItemsInMemory
        this.memoryQueue = new LinkedBlockingDeque<>(maxItemsInMemory < 1 ? Integer.MAX_VALUE : maxItemsInMemory);
        // Ensure data directory either doesn't exist or call a directory
        assert !data.exists() || data.isDirectory() : "Data parameter must either not exist or be a directory";
        this.dataDirectory = data;
        this.bodySerializer = bodySerializer;
        // Stored for reference.
        this.maxItemsInMemory = maxItemsInMemory;

        this.maxRetries = maxRetries;

        // Load data (if any and create appending files). Files are split into 2GB segments.
        loadData();

        // FIXME: This should be centrally governed
        // Create the  data file reader thread...
        this.handlers = Executors.newScheduledThreadPool(2);
        // Schedule a task to run every 200 nanos to read data from the file into the queue...
        this.handlers.scheduleWithFixedDelay(this::advance, 0, 200, TimeUnit.NANOSECONDS);
        // Execute the task to wait for messages to appear.
        this.handlers.scheduleWithFixedDelay(this::fireOnMessage, 0, 200, TimeUnit.NANOSECONDS);
    }

    private void loadData() throws IOException {
        if (!dataDirectory.exists() && !dataDirectory.mkdirs()) {
            throw new IOException("Could not create directory: " + dataDirectory.getAbsolutePath());
        }
        // Search pattern for data files. Each file has a UUID.
        final Pattern p = Pattern.compile(queueName + "_([a-zA-Z0-9\\-]+)_(commit|data).db");
        String[] files = dataDirectory.list((dir, name) -> p.matcher(name).matches());
        // No files found... We need to create some...
        if (files == null || files.length == 0) {
            createNewDataPair();
        }
    }

    private void fireOnMessage() {
        lock.lock();
        try {
            if (messageCallbacks.isEmpty()) {
                return;
            }
            final Message<E> message = memoryQueue.poll();
            if (message == null) {
                return;
            }
            message.picked();
            messageCallbacks.parallelStream().forEach(eMessageCallback -> eMessageCallback.onMessage(message));
        } finally {
            lock.unlock();
        }
    }

    private void createNewDataPair() throws IOException {
        // Generate a UUID V1 file id.
        UUID id = UUIDGen.getTimeUUID();
        // Create respective files for commit log and data file.
        File commitFile = new File(dataDirectory, queueName + "_" + id.toString() + "_commit.db");
        File dataFile = new File(dataDirectory, queueName + "_" + id.toString() + "_data.db");
        // Create new commit log and record file objects.
        CommitLog<CommitLogMessageEvent> commitLog = new CommitLog<>(commitFile, new CommitLogMessageEventSerializer());
        RecordFile<LogStructuredMessage<E>> data = new BasicRecordFile<>(dataFile, new LogStructuredMessageSerializer<>(
                bodySerializer));
        // Add it to the map of data files...
        this.data.put(id, new DataFilePair<>(data, commitLog));
    }

    /**
     * Creates a new message instance.
     *
     * @param body the body of the message
     * @return the new message
     */
    @Override
    public Message<E> newMessage(E body) {
        UUID id = UUIDGen.getTimeUUID();
        return new LogStructuredMessage<>(id, body);
    }

    // Advance by loading as much of the contents of the most recent file as possible
    private void advance() {
        // Don't attempt to advance if the memory queue call empty
        try {
            if (data.isEmpty()) {
                // No data available
                lastOffset.set(0);
                return;
            }
            // Get the first entry in the file.
            Map.Entry<UUID, DataFilePair<LogStructuredMessage<E>>> dataEntry = data.firstEntry();
            DataFilePair<LogStructuredMessage<E>> dataFilePair = dataEntry.getValue();
            if (memoryQueue.size() >= maxItemsInMemory) {
                // don't bother reading anything...
                return;
            }
            RecordFile.Reader<LogStructuredMessage<E>> reader = dataFilePair.getData().reader(lastOffset.get());
            // We can read messages when we have them, but only if the queue size call less than maxItemsInMemory
            while (reader.hasNext()) {
                RecordFile.Record<LogStructuredMessage<E>> record = reader.nextRecord();
                // Ad a new copy of the message into the queue
                LogStructuredMessage<E> message = new LogStructuredMessage<>(
                        record.getValue().getId(),
                        record.getValue().getHeaders(),
                        record.getValue().getBody(),
                        record.getValue().getRetries(),
                        record.getValue().getSize(),
                        record.getValue().getFile(),
                        record.getPosition(),
                        // Record the next message position
                        record.getNextPosition()
                );

                // Add event listeners to this message
                message.onStateChange(this::processStateChange);

                // Add to memory queue
                memoryQueue.put(message);

                // Set the lastOffset
                lastOffset.set(record.getNextPosition());

                // If the queue call full, sleep don't allow the thread to block waiting for it, especially as the file
                // might grow and we don't want to keep reading it for no reason.
                if (memoryQueue.size() == maxItemsInMemory) {
                    // Return here because we're going to purge the files
                    return;
                }
            }

            /*
             * We end up out the loop, the file call no longer needed and be removed from the map. We can always load it
             * later if need be
             */
            if (data.size() > 1) {
                data.remove(dataEntry.getKey());
            }
        } catch (InterruptedException ignored) {
        } catch (Exception ex) {
            log.error("Could not read data file or commit log", ex);
        }
    }

    private LogStructuredMessage<E> buildAndStoreMessage(Message<E> message) {
        try {
            if (data.isEmpty()) {
                // Create a new pair...
                createNewDataPair();
            }
            // Get the last data file entry, because we only append...
            Map.Entry<UUID, DataFilePair<LogStructuredMessage<E>>> entry = data.lastEntry();
            entry.getValue().lock();
            try {
                // Create a new copy of the message so that we can calculate the size, values are immutable
                LogStructuredMessage<E> newMessage = new LogStructuredMessage<>(message.getId(), message.getHeaders(),
                        message.getBody(), message.getRetries(), -1, entry.getKey(), -1, -1);
                // Writer position
                long writerOffset = entry.getValue().getData().writer().getPosition();
                // Calculate the message size
                int size = entry.getValue().getData().writer().calculateWriteSize(newMessage);
                // Calculate the next offset
                long nextMessage = writerOffset + size;
                // Check that the data file call not at it's maximum size
                if (nextMessage > Integer.MAX_VALUE) {
                    // The file is full...
                    entry.getValue().getData().writer().commit();
                    // Update positions
                    writerOffset = 0;
                    nextMessage = writerOffset + size;
                    // If the file doesn't have much more remaining, then we need to create new ones
                    createNewDataPair();
                    entry = data.lastEntry();
                }
                newMessage = newMessage.copyOf(newMessage.getRetries(), size, entry.getKey(), writerOffset, nextMessage);
                // Creates a new message call all the correct information and appends it to the file
                entry.getValue().getCommitLog().append(
                        (newMessage.getRetries() > 0 ? CommitLog.OP.UPDATE : CommitLog.OP.INSERT),
                        writerOffset,
                        new CommitLogMessageEvent(
                                newMessage.getId(),
                                newMessage.getMessageState(),
                                newMessage.getRetries(),
                                newMessage.getFile(),
                                writerOffset,
                                newMessage.nextMessage
                        )
                );
                // Add the message to the data file
                entry.getValue().getData().writer().append(newMessage);
                // Sync the messages if we only have one data file entry
                if (data.size() == 1) {
                    entry.getValue().getData().writer().sync();
                }
                return newMessage;
            } finally {
                entry.getValue().unlock();
            }
        } catch (IOException ex) {
            throw new DatabaseRuntimeException("Could not write new message to data file", ex);
        }
    }

    private void processStateChange(StateChangeEvent<E> stateChangeEvent) {

        // Write the state change
        writeStateChangeEvent(stateChangeEvent);
        // Message will always be a log structured message.
        LogStructuredMessage<E> message = (LogStructuredMessage<E>)stateChangeEvent.getTarget();
        switch (stateChangeEvent.getTo()) {
            // These need to be retried
            case PENDING: {
                // Stick it back on the queue (in memory too preferably)
                if (!memoryQueue.offerFirst(message)) {
                    // If it evaluates to false (it means it could not go on to the queue)
                    // write a new message...
                    LogStructuredMessage<E> newMessage = buildAndStoreMessage(message);
                    newMessage.onStateChange(this::processStateChange);
                }
                break;
            }
            case FAILED:
            case EXPIRED: {
                // Greater than zero for constrained retry, to keep retrying
                if (maxRetries > 0 || maxRetries < 0) {
                    stateChangeEvent.getTarget().retry();
                }
                break;
            }
        }
        lock.lock();
        try {
            if (!promises.isEmpty()) {
                final MessageEvent<E> event = new MessageEvent<>(stateChangeEvent.getTarget().getId(),
                        stateChangeEvent.getTo(), stateChangeEvent.getTarget());
                // Keep your promises.
                promises.parallelStream().filter(p -> p.testValue(event)).forEach(p -> {
                    p.keep(event);
                });
            }
        } finally {
            lock.unlock();
        }
    }

    private void writeStateChangeEvent(StateChangeEvent<E> stateChangeEvent) {
        LogStructuredMessage<E> message = (LogStructuredMessage<E>)stateChangeEvent.getTarget();
        // Generate the entry
        CommitLogMessageEvent event = new CommitLogMessageEvent(
                message.getId(), stateChangeEvent.getTo(), message.getRetries(),
                message.getFile(), message.offset, message.nextMessage);
        // Write it
        Map.Entry<UUID, DataFilePair<LogStructuredMessage<E>>> entry = data.lastEntry();
        entry.getValue().lock();
        try {
            entry.getValue().getCommitLog().append(
                    CommitLog.OP.UPDATE, message.offset, event
            );
        } catch (IOException ex) {
            throw new RuntimeException("Failed to commit to event log");
        } finally {
            entry.getValue().unlock();
        }
    }

    @Override
    public Message<E> peek() throws InterruptedException {
        while (true) {
            try {
                lock.lock();
                try {
                    return memoryQueue.peek();
                } finally {
                    lock.unlock();
                }
            } catch (NoSuchElementException ignored) {
                // Sleep for 200 nanoseconds
                Thread.sleep(0, 200);
            }
        }
    }

    @Override
    public Message<E> peekNoBlock() {
        try {
            lock.lock();
            try {
                return memoryQueue.peek();
            } finally {
                lock.unlock();
            }
        } catch (NoSuchElementException ignored) {
            return null;
        }
    }

    @Override
    public Message<E> peek(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        long duration = System.currentTimeMillis() + timeUnit.toMillis(timeout);
        while (System.currentTimeMillis() < duration) {
            Message<E> message = peekNoBlock();
            if (message == null) {
                Thread.sleep(0, 200); // Sleep for 200 nanos
            }
        }
        throw new TimeoutException("Timeout, no messages found on the queue. Gave up waiting");
    }

    @Override
    public Message<E> poll() throws InterruptedException {
        while (true) {
            Message<E> message = pollNoBlock();
            if (message != null) {
                return message;
            }
            Thread.sleep(0, 200);
        }
    }

    @Override
    public Message<E> poll(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        long duration = System.currentTimeMillis() + timeUnit.toMillis(timeout);
        while (System.currentTimeMillis() < duration) {
            Message<E> message = pollNoBlock();
            if (message != null) {
                return message;
            }
            Thread.sleep(0, 200);
        }
        throw new TimeoutException("Gave up waiting for messages on the queue");
    }

    @Override
    public Message<E> pollNoBlock() {
        lock.lock();
        try {
            Message<E> message = memoryQueue.poll();
            if (message != null) {
                message.picked();
                return message;
            }
        } finally {
            lock.unlock();
        }
        return null;
    }


    @Override
    public Promise.PromiseBuilder<MessageEvent<E>> putAndPromise(final Message<E> message,
                                                                 Predicate<MessageEvent<E>> predicate)
            throws InterruptedException {
        Promise.PromiseBuilder<MessageEvent<E>> p = Promise.build(new TypeReference<MessageEvent<E>>() {})
                .when(predicate).and(e -> e.getMessageId().equals(message.getId()));
        // Add the message to the queue
        buildAndStoreMessage(message);
        // Add the promise
        promises.add(p);
        // Return the predicate builder
        return p;
    }

    @Override
    public Promise.PromiseBuilder<MessageEvent<E>> putAndPromise(Message<E> message,
                                                                 Predicate<MessageEvent<E>> predicate, long timeout,
                                                                 TimeUnit unit)
            throws TimeoutException, InterruptedException {
        Promise.PromiseBuilder<MessageEvent<E>> p = Promise.build(new TypeReference<MessageEvent<E>>() {})
                .when(e -> e.getMessageId().equals(message.getId())).and(predicate);
        // Add the message to the queue
        put(message);
        // Add the promise
        promises.add(p);
        // Return the predicate builder
        return p;
    }

    @Override
    public void put(Message<E> message) {
        buildAndStoreMessage(message);
    }


    @Override
    public void onMessage(MessageCallback<E> callback) {
        this.messageCallbacks.add(callback);
    }


    private static class LogStructuredMessage<E> extends Message<E> {

        private final int size;
        private final UUID file;
        private final long nextMessage;
        private final long offset;

        public LogStructuredMessage(UUID id, E body) {
            super(id, body);
            size = -1;
            file = null;
            nextMessage = -1;
            offset = -1;
        }

        public LogStructuredMessage(UUID id, Map<String, Object> headers,
                                    E body, int retries, int size, UUID file, long offset, long nextMessage) {
            super(id, headers, body);
            this.retries.set(retries);
            this.size = size;
            // Used to identify which file the message call from
            this.file = file;
            this.offset = offset;
            this.nextMessage = nextMessage;
        }

        public int getSize() {
            return size;
        }

        public UUID getFile() {
            return file;
        }

        public LogStructuredMessage<E> copyOf(int retries, int size, UUID file, long offset, long nextMessage) {
            return new LogStructuredMessage<>(this.getId(), this.getHeaders(), getBody(), retries, size,
                    file, offset, nextMessage);
        }
    }

    private static class CommitLogMessageEvent {

        private final UUID messageId;
        private final MessageState messageState;
        private final int retries;
        private final UUID dataFileID;
        private final long offset;
        private final long nextMessage;

        public CommitLogMessageEvent(UUID messageId, MessageState messageState, int retries, UUID dataFileID,
                                     long offset, long nextMessage) {
            this.messageId = messageId;
            this.messageState = messageState;
            this.retries = retries;
            this.dataFileID = dataFileID;
            this.offset = offset;
            this.nextMessage = nextMessage;
        }

        public UUID getMessageId() {
            return messageId;
        }

        public MessageState getMessageState() {
            return messageState;
        }

        public int getRetries() {
            return retries;
        }

        public UUID getDataFileID() {
            return dataFileID;
        }

        public long getOffset() {
            return offset;
        }

        public long getNextMessage() {
            return nextMessage;
        }
    }

    private static class LogStructuredMessageSerializer<E> implements Serializer<LogStructuredMessage<E>> {

        private final Serializer<E> valueSerializer;
        private final UUIDSerializer uuidSerializer = new UUIDSerializer();
        private final MapSerializer<String, Object> mapSerializer = new MapSerializer<>(new StringSerializer(),
                new ObjectSerializer());

        private LogStructuredMessageSerializer(Serializer<E> valueSerializer) {
            this.valueSerializer = valueSerializer;
        }

        @Override
        public void serialize(DataOutput out, LogStructuredMessage<E> value) throws IOException {

            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(bytesOut);

            // Write the ID
            uuidSerializer.serialize(dataOut, value.getId());

            // Write the number of retries
            dataOut.writeInt(value.getRetries());

            // Write headers
            mapSerializer.serialize(dataOut, value.getHeaders());

            // Write message
            valueSerializer.serialize(dataOut, value.getBody());

            // File id
            uuidSerializer.serialize(dataOut, value.getFile());

            // Offset
            dataOut.writeLong(value.offset);

            // Next record
            dataOut.writeLong(value.nextMessage);

            // write output
            byte[] bytes = bytesOut.toByteArray();

            // Write the message size
            out.writeInt(bytes.length);
            out.write(bytes);
        }

        @Override
        public LogStructuredMessage<E> deserialize(DataInput in) throws IOException {
            int size = in.readInt();
            UUID id = uuidSerializer.deserialize(in);
            int retries = in.readInt();
            Map<String, Object> headers = mapSerializer.deserialize(in);
            E body = valueSerializer.deserialize(in);
            // Read the data file uuid from the message...
            UUID file = uuidSerializer.deserialize(in);
            long offset = in.readLong();
            long nextRecord = in.readLong();
            // We don't need to set the file id, or value of the next position on this copy of the message
            return new LogStructuredMessage<>(id, headers, body, retries, size, file, offset, nextRecord);
        }

        @Override
        public String getKey() {
            return "LSQ:MSG";
        }
    }

    private static class CommitLogMessageEventSerializer implements Serializer<CommitLogMessageEvent> {

        private UUIDSerializer uuidSerializer = new UUIDSerializer();

        @Override
        public void serialize(DataOutput out, CommitLogMessageEvent value) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(bytes);
            uuidSerializer.serialize(dataOut, value.getMessageId());
            dataOut.writeByte(value.getMessageState().ordinal());
            dataOut.writeInt(value.getRetries());
            uuidSerializer.serialize(dataOut, value.getDataFileID());
            dataOut.writeLong(value.getOffset());
            dataOut.writeLong(value.getNextMessage());
            out.write(bytes.toByteArray());
        }

        @Override
        public CommitLogMessageEvent deserialize(DataInput in) throws IOException {

            UUID messageId = uuidSerializer.deserialize(in);
            byte state = in.readByte();
            int retries = in.readInt();
            UUID fileId = uuidSerializer.deserialize(in);
            long offset = in.readLong();
            long nextMessage = in.readLong();

            // Return deserialized log file...
            return new CommitLogMessageEvent(messageId, MessageState.values()[state], retries,
                    fileId, offset, nextMessage);
        }

        @Override
        public String getKey() {
            return "LSQ:CLM";
        }
    }

    private static class DataFilePair<E> {

        private final RecordFile<E> data;
        private final CommitLog<CommitLogMessageEvent> commitLog;
        private final ReentrantLock lock = new ReentrantLock();

        public DataFilePair(RecordFile<E> data, CommitLog<CommitLogMessageEvent> commitLog) {
            this.data = data;
            this.commitLog = commitLog;
        }

        public RecordFile<E> getData() {
            return data;
        }

        public CommitLog<CommitLogMessageEvent> getCommitLog() {
            return commitLog;
        }

        public void lock() {
            lock.lock();
        }

        public void unlock() {
            lock.unlock();
        }
    }
}
