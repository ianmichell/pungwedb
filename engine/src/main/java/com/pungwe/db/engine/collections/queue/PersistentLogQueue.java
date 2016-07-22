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
import com.pungwe.db.engine.collections.btree.AbstractBTreeMap;
import com.pungwe.db.engine.collections.btree.BTreeMap;
import com.pungwe.db.engine.collections.btree.ImmutableBTreeMap;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// FIXME: Needs a bloom filter!

/**
 * @param <E>
 */
public class PersistentLogQueue<E> implements Queue<E> {

    /*
     * We will need to provide some for of locking to ensure that we can flush the nextGeneration index to disk along
     * with the fact that we will be making modifications to each message as they run through the queue.
     */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Memory store is a btree that is used to ensure message order by UUID, track changes and ensure
     * that we don't get repeat messages running through the queue.
     * <p>
     * Messages work from old to to new, so this tree will be where messages are inserted. When it is full it will be
     * pushed to disk, so that data is not lost.
     * <p>
     * It is also managed by a CommitLog that is cycled each time the tree is recreated...
     */
    private BTreeMap<UUID, Message<E>> nextGeneration = new BTreeMap<>(UUID::compareTo, 100);

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
     * Auto acknowledgement. If this is true, then messages are marked as acknowledged as soon as they are pulled off
     * the queue.
     */
    private boolean autoAcknowledge = false;

    /**
     * Max retries... If the message fails delivery, it needs to be retried
     */
    private final int maxRetries = 0;

    private Message<E> next;

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
             * replication queues that the messages are picked up immediately, or within a few hours. Lots of
             * traffic will cause disk flushes and slow disk io down.
             *
             * If the current next element is not null, then we simply want a newer message, so we loop
             * through until we find one, not ideal but with next as a starting point it should be a lot quicker
             *
             */
            Map.Entry<UUID, Message<E>> currentEntry = next == null ? tree.firstEntry() :
                    tree.higherEntry(next.getId());

            // If currentEntry is null, then we are not going to find anything in this tree
            if (currentEntry == null) {
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
             * We need to search the trees for an older entry just to be on the safe side... This will happen in
             * reverse order, so that one of the newer trees will find it if at all. There should never be billions
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
        // Ensure next is not null
        if (next == null) {
            return;
        }
        listeners.parallelStream().forEach(eMessageCallback -> eMessageCallback.onMessage(next));
    }

    private Message<E> findOlderPendingMessageInTrees(AbstractBTreeMap<UUID, Message<E>> current, UUID id) {
        // Check the current tree for an older record
        Message<E> message = findOlderPendingMessageInTree(current, id);
        // If the message is not null. Then return it, as it's older and pending...
        if (message != null) {
            return message;
        }

        // If current is the next gen map, then we have the newest available map...
        if (BTreeMap.class.isAssignableFrom(current.getClass())) {
            // If older entry is null, then we don't have an older entry
            return null;
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
        // This tree is finished, we can remove it
        previousGenerations = Arrays.copyOfRange(previousGenerations, 1, previousGenerations.length);
        // We can purge the tree, because any retries are always appended to the queue... So delete the record files
        // kill the object and we're done.
    }

    /**
     *
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
        return new QueueMessage(next.getId(), next.getHeaders(), next.getBody(), next.getRetries());
    }

    /**
     *
     * @param timeout the timeout for blocking for a new message
     * @param timeUnit the unit of measure for time (milliseconds, seconds, minutes, days).
     *
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
        // Make sure that nano seconds or microseconds are not set as the time unit...
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
        return new QueueMessage(next.getId(), next.getHeaders(), next.getBody(), next.getRetries());
    }

    /**
     *
     * @return
     * @throws InterruptedException
     */
    @Override
    public Message<E> poll() throws InterruptedException {
        lock.writeLock().lock();
        try {
            Message<E> n = peek();
            return buildMessage(n);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     *
     * @param timeout the amount of time to wait until there is a new message
     * @param timeUnit the unit of measure for the timeout.
     *
     * @return
     * @throws TimeoutException
     * @throws InterruptedException
     */
    @Override
    public Message<E> poll(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        lock.writeLock().lock();
        try {
            Message<E> n = peek(timeout, timeUnit);
            return buildMessage(n);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Message<E> buildMessage(final Message<E> from) {
        // Fire the event...
        Message<E> to = new QueueMessage(from.getId(), from.getHeaders(), from.getBody(), from.getRetries(), event -> {
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
                }
                default: {
                    // We don't want to fire anymore events here. Simply update the status
                    replace.setMessageState(event.getTo());
                    break;
                }
            }
            nextGeneration.put(replace.getId(), replace);
        });
        // from comes from peek, so is already a queue message
        if (autoAcknowledge) {
            // Acknowledge the message...
            to.acknowledge();
        } else {
            // otherwise set it to processing...
            to.processing();
        }
        return to;
    }

    @Override
    public Promise<MessageEvent<E>> put(Message<E> message) {
        return null;
    }

    @Override
    public void onMessage(MessageCallback<E> callback) {

    }

    private class QueueMessage extends Message<E> {

        public QueueMessage(UUID id, Map<String, Object> headers, E body, int retries) {
            super(id, headers, body);
            this.retries.set(retries);
        }

        public QueueMessage(UUID id, Map<String, Object> headers, E body, int retries,
                            StateChangeEventListener<E> listener) {
            super(id, headers, body);
            this.onStateChange(listener);
            this.retries.set(retries);
        }

        protected void setMessageState(MessageState state) {
            this.messageState = state;
        }
    }
}
