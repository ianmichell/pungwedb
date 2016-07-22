package com.pungwe.db.core.concurrent;

import com.pungwe.db.core.error.PromiseException;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by ian on 23/06/2016.
 */
public class Promise<T> {

    // Used to lock the callbacks for thread safety as we only want to fire each one, once...
    private final ReentrantLock lock = new ReentrantLock();
    private Future<T> future;
    private FailCallback failCallback;
    private DoneCallback<T> doneCallback;
    @SuppressWarnings("unused")
    private Callable<T> callable;

    private Promise() {
    }
    
    /**
     * This is the action. When I promise this thing. Then do something or when it fails do something else.
     *
     * @param callable the callable to be executed by the promise
     * @param <T>      the return type.
     * @return the current promise.
     */
    public static <T> Promise<T> when(final Callable<T> callable) {
        final Promise<T> promise = new Promise<>();
        promise.callable = () -> {
            try {
                T v = callable.call();
                promise.lock.lock();
                try {
                    if (promise.doneCallback != null) {
                        promise.doneCallback.call(v);
                        promise.doneCallback = null;
                        promise.failCallback = null;
                    }
                } finally {
                    promise.lock.unlock();
                }
                return v;
            } catch (Exception ex) {
                promise.lock.lock();
                try {
                    if (promise.failCallback != null) {
                        promise.failCallback.call(new PromiseException(ex));
                        promise.failCallback = null;
                        promise.doneCallback = null;
                    }
                    throw ex; // throw the exception anyway
                } finally {
                    promise.lock.unlock();
                }
            }
        };
        // Submit the promise for execution
        promise.future = PromiseExecutor.getInstance().submit(callable);
        return promise;
    }

    /**
     * Callback fired when the promise is completed. This is used as an alternative to get() in that it is fired
     * by the worker thread when it has completed processing. This means that you can use a promise to execute some
     * work and get notified when it has been completed.
     * <p>
     * <code>Promise.when(() -> ... ).then((result) -> ...).resolve();</code>
     *
     * @param callback the callback on completion.
     * @return the current promise.
     */
    public Promise<T> then(DoneCallback<T> callback) {
        this.doneCallback = callback;
        fireIfDone();
        return this;
    }

    /**
     * Callback on failure. For asynchronous promises this method is called when an exception occurs during promise
     * execution.
     *
     * @param callback the failure callback
     * @return the current promise.
     */
    public Promise<T> fail(FailCallback callback) {
        this.failCallback = callback;
        fireIfDone();
        return this;
    }

    /**
     * Utility method that checks if the promise is completed and tries to fire then or fail (if they are non null).
     */
    private void fireIfDone() {
        if (future.isDone()) {
            try {
                T result = future.get();
                lock.lock();
                try {
                    if (doneCallback != null) {
                        doneCallback.call(result);
                        // Set to null so that they are not fired more than once
                        doneCallback = null;
                        failCallback = null;
                    }
                } finally {
                    lock.unlock();
                }
            } catch (Exception ex) {
                lock.lock();
                try {
                    if (failCallback != null) {
                        failCallback.call(new PromiseException(ex));
                        // Set to null so that they are not fired more than once
                        failCallback = null;
                        doneCallback = null;
                    }
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    /**
     * Waits for the promise to be fulfilled. This method doesn't return anything, but will block the current thread
     * until the promise has finished being executed.
     */
    public void resolve() {
        while (!future.isDone()) {
            try {
                Thread.sleep(0, 200);
            } catch (InterruptedException ex) {
            }
        }
        fireIfDone();
    }

    /**
     * Waits for the promise to be fulfilled. This method doesn't return anything but will block the current thread
     * up to the timeout. If the timeout is reached, then a PromiseException will be thrown to illustrate this.
     * <p>
     * Milliseconds are the lowest measurement allowed for the timeout. If a lower unit is specified an IllegalArgument
     * exception will be thrown.
     *
     * @param duration the duration of the timeout
     * @param unit     the unit of the duration.
     * @throws TimeoutException         if the timeout is reached.
     * @throws CancellationException    if the task was cancelled before the promise could be resolved.
     * @throws InterruptedException     if resolution is interrupted
     * @throws IllegalArgumentException if the time unit is invalid or the duration is less than or equal to 0.
     */
    public void resolve(long duration, TimeUnit unit) throws TimeoutException, InterruptedException,
            CancellationException {
        // Ensure the duration is above 0
        if (duration < 1) {
            throw new IllegalArgumentException("Duration must be greater than 0...");
        }
        // Make sure that nano seconds or microseconds are not set as the time unit...
        if (TimeUnit.NANOSECONDS.equals(unit) || TimeUnit.MICROSECONDS.equals(unit)) {
            throw new IllegalArgumentException("Lowest supported time unit is milliseconds");
        }
        // Calculate what time the timeout will occur.
        long waitFor = System.currentTimeMillis() + unit.toMillis(duration);
        // Loop, check and wait until the timeout has been reached.
        while (System.currentTimeMillis() < waitFor) {
            // Are we done yet?
            if (!future.isDone()) {
                Thread.sleep(0, 200);
            } else if (future.isCancelled()) {
                throw new CancellationException("Could not resolve as the task was cancelled");
            } else {
                // See if we can fire the callbacks.
                fireIfDone();
                // Return is we have reached the timeout
                return;
            }
        }
        throw new TimeoutException("Timeout exceeded!");
    }

    /**
     * Waits for the promise value to be fulfilled and returns it. This an alternative to then() and fail()
     *
     * @return the value of the promise
     * @throws PromiseException      an exception when the promise fails...
     * @throws CancellationException if the promise is cancelled prematurely
     * @throws InterruptedException  if resolution is interrupted
     */
    public T get() throws PromiseException, CancellationException, InterruptedException {
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw new PromiseException(e);
        }
    }

    /**
     * Waits up to the specified timeout for the promise to be fulfilled and returns it. This allows promise resolution
     * without "then" and "fail" callbacks.
     *
     * @param duration the duration of the timeout
     * @param unit the time unit of measure
     * @return the result of the promise
     *
     * @throws PromiseException the error raised during execution
     * @throws TimeoutException when the timeout has been reached
     * @throws InterruptedException if resolution was interrupted
     * @throws CancellationException if the promise was cancelled prematurely
     * @throws IllegalArgumentException if the duration is less than 1 or the time unit is less than milliseconds.
     */
    public T get(long duration, TimeUnit unit) throws PromiseException, TimeoutException, InterruptedException,
            CancellationException {
        // Ensure the duration is above 0
        if (duration < 1) {
            throw new IllegalArgumentException("Duration must be greater than 0...");
        }
        // Make sure that nano seconds or microseconds are not set as the time unit...
        if (TimeUnit.NANOSECONDS.equals(unit) || TimeUnit.MICROSECONDS.equals(unit)) {
            throw new IllegalArgumentException("Lowest supported time unit is milliseconds");
        }
        try {
            return future.get(duration, unit);
        } catch (ExecutionException e) {
            throw new PromiseException(e);
        }
    }

    public interface FailCallback {
        void call(Throwable error);
    }

    public interface DoneCallback<T> {
        void call(T result);
    }
}
