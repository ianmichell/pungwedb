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
package com.pungwe.db.core.collections.queue;

import com.pungwe.db.core.concurrent.Promise;
import com.pungwe.db.core.utils.UUIDGen;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @param <E>
 */
public interface Queue<E> {

    /**
     * Checks to see if there is an unacknowledged message on the queue. If there is no message on the queue
     * it will block until there is one.
     *
     * @return the next available message in the queue.
     */
    Message<E> peek() throws InterruptedException;

    /**
     * Checks to see if there is an unacknowledged message on the queue. If there is no message on the queue
     * it waits until the value of time out.
     *
     * @param timeout the timeout for blocking for a new message
     * @param timeUnit the unit of measure for time (milliseconds, seconds, minutes, days).
     *
     * @return the next available message on the queue when available
     */
    Message<E> peek(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException;

    /**
     * Retrieves a message off the queue and either acknowledges the message or marks it as
     * in progress if auto-acknowledgement is turned on.
     *
     * If there are no messages on the queue, it will wait until there is one.
     *
     * @return the next available message in the queue.
     */
    Message<E> poll() throws InterruptedException;

    /**
     * Retrieves a message off the queue and either acknowledges the message or marks it
     * in progress if auto-acknowledgement is turned on.
     *
     * If there are no messages the method will wait until the timeout before throwing an exception.
     *
     * @param timeout the amount of time to wait until there is a new message
     * @param timeUnit the unit of measure for the timeout.
     *
     * @return the next message.
     */
    Message<E> poll(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException;

    /**
     * Puts a message on the queue and returns a promise object so that delivery completion and failures can be
     * monitored.
     *
     * Note that if a message fails it will retry up to the specified threshold. Once the retry threshold is reached
     * then the fail method on the promise will be triggered. If retry count is 0, then it will trigger the failure
     * callback immediately if there is an error.
     *
     * If the retry threshold is below zero, it's assumed that there will be an infinite retry and the message will
     * block the queue until it's successfully delivered. This is very useful when performing replication as it will
     * guarantee that a message will be delivered.
     *
     * <code>
     *     <pre>
     *         queue.put(message).then((e) -> ... ).fail((e) -> ... );
     *     </pre>
     * </code>
     *
     * @param message the message to be placed on the queue
     *
     * @return the delivery promise.
     */
    Promise<MessageEvent<E>> put(Message<E> message);

    /**
     * Executes a callback when the next message is available. This works as an alternative to poll for asynchronous
     * programming as it does not block the current thread from moving on.
     *
     * On execution of the callback the message is marked as either IN PROGRESS or ACKNOWLEDGED depending on the
     * delivery preference of the queue.
     *
     * @param callback the callback to be executed when a new message is available.
     */
    void onMessage(MessageCallback<E> callback);

    /**
     * This class holds event information about a message when it is marked as acknowledged.
     *
     * @param <E> the message body type
     */
    final class MessageEvent<E> {

        private final MessageState state;
        private final E target;

        public MessageEvent(MessageState state, E target) {
            this.state = state;
            this.target = target;
        }

        public MessageState getState() {
            return state;
        }

        public E getTarget() {
            return target;
        }
    }

    class StateChangeEvent<E> {

        final MessageState from;
        final MessageState to;
        final Message<E> target;

        public StateChangeEvent(MessageState from, MessageState to, Message<E> target) {
            this.from = from;
            this.to = to;
            this.target = target;
        }

        public MessageState getFrom() {
            return from;
        }

        public MessageState getTo() {
            return to;
        }

        public Message<E> getTarget() {
            return target;
        }
    }

    /**
     * Abstract base class, that does not allow generic use, as the message object should be immutable once it's on the
     * queue.
     *
     * @param <E> the message body type.
     */
    abstract class Message<E> {
        private UUID id;
        protected MessageState messageState = MessageState.PENDING;
        private Map<String, Object> headers;
        private List<StateChangeEventListener<E>> stateChangeEventListeners = new LinkedList<>();
        private E body;
        protected final AtomicInteger retries = new AtomicInteger();

        protected Message(UUID id, E body) {
            this.id = id;
            this.body = body;
            this.headers = new LinkedHashMap<>();
        }

        protected Message(UUID id, Map<String, Object> headers, E body) {
            this.id = id;
            this.headers = new LinkedHashMap<>();
            this.headers.putAll(headers);
            this.body = body;
        }

        public final void addHeader(String name, Object value) {
            this.headers.put(name, value);
        }

        public final Object getHeader(String name) {
            return this.headers.get(name);
        }

        public UUID getId() {
            return id;
        }

        public MessageState getMessageState() {
            return messageState;
        }

        public Map<String, Object> getHeaders() {
            return headers;
        }

        public E getBody() {
            return body;
        }

        public int getRetries() {
            return retries.get();
        }

        public void incrementRetries() {
            retries.incrementAndGet();
        }

        public final void retry() {
            // We don't fire the change events.
            incrementRetries();
            this.messageState = MessageState.PENDING;
        }

        public final void acknowledge() {
            MessageState previousState = messageState;
            this.messageState = MessageState.ACKNOWLEDGED;
            StateChangeEvent<E> event = new StateChangeEvent<>(previousState, messageState, this);
            executeListeners(event);
        }

        public final void processing() {
            MessageState previousState = messageState;
            this.messageState = MessageState.PROCESSING;
            StateChangeEvent<E> event = new StateChangeEvent<>(previousState, messageState, this);
            executeListeners(event);
        }

        public final void failed() {
            MessageState previousState = messageState;
            this.messageState = MessageState.FAILED;
            StateChangeEvent<E> event = new StateChangeEvent<>(previousState, messageState, this);
            executeListeners(event);
        }

        public final void expire() {
            MessageState previousState = messageState;
            this.messageState = MessageState.EXPIRED;
            StateChangeEvent<E> event = new StateChangeEvent<>(previousState, messageState, this);
            executeListeners(event);
        }

        private final void executeListeners(StateChangeEvent<E> event) {
            // Fire to all the event listeners in parallel...
            stateChangeEventListeners.parallelStream().forEach(listener -> listener.onStateChange(event));
        }

        protected final void onStateChange(StateChangeEventListener<E> callback) {
            stateChangeEventListeners.add(callback);
        }

        public boolean isPending() {
            return messageState.equals(MessageState.PENDING);
        }
    }

    interface MessageCallback<E> {
        void onMessage(Message<E> message);
    }

    interface StateChangeEventListener<E> {
        void onStateChange(StateChangeEvent<E> message);
    }

    enum MessageState {
        PENDING, PROCESSING, ACKNOWLEDGED, EXPIRED, FAILED
    }
}
