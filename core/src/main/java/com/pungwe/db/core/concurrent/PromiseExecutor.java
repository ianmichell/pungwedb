package com.pungwe.db.core.concurrent;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

final class PromiseExecutor {

    private static PromiseExecutor INSTANCE;

    private final ReentrantLock lock = new ReentrantLock();
    private final ExecutorService executorService;

    private PromiseExecutor() {
        // We want a work stealing pool to take full advantage of our hardware
        this.executorService = Executors.newWorkStealingPool();
    }

    private PromiseExecutor(ExecutorService executorService) {
        this.executorService = executorService;
    }

    // No need to make this accessible outside the package
    @SafeVarargs
    final <T> Future<T> submit(Promise<T> promise, Callable<T> callable, final PromiseMonitor<T>... monitors) {
        lock.lock();
        try {
            final Future<T> future = executorService.submit(callable);
            if (monitors.length == 1) {
                monitorPromise(promise, future, monitors[0]);
            }
            return future;
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void monitorPromise(final Promise<T> promise, Future<T> future, PromiseMonitor<T> monitor) {
        lock.lock();
        try {
            executorService.submit(() -> {
                // Set the promise to have a running monitor...
                try {
                    for (int i = 0; i < 5; i++) {
                        if (!promise.promiseMonitorRunning.get()) {
                            return;
                        }
                        PromiseMonitorEvent<T> event = null;
                        try {
                            T result = future.get(200, TimeUnit.NANOSECONDS);
                            event = new PromiseMonitorEvent<>(result, future);
                        } catch (ExecutionException | CancellationException ex) {
                            event = new PromiseMonitorEvent<>(ex, future);
                        } catch (TimeoutException | InterruptedException ignored) {
                            // do nothing here either, just retry
                            continue;
                        }
                        // Execute the monitors

                        try {
                            monitor.call(event);
                            return;
                        } finally {
                            // We don't want to run this anymore...
                            promise.promiseMonitorRunning.set(false);
                        }
                    }
                } finally {
                    if (promise.promiseMonitorRunning.get()) {
                        monitorPromise(promise, future, monitor);
                    }
                }
            });
        } finally {
            lock.unlock();
        }
    }

    public static PromiseExecutor getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new PromiseExecutor();
        }
        return INSTANCE;
    }

    public static PromiseExecutor newInstanceWith(ExecutorService executorService) {
        return new PromiseExecutor(executorService);
    }

    public interface PromiseMonitor<T> {
        void call(PromiseMonitorEvent<T> future);
    }

    public class PromiseMonitorEvent<T> {
        private final Throwable error;
        private final T result;
        private final Future future;

        public PromiseMonitorEvent(Throwable error, Future future) {
            this.error = error;
            this.future = future;
            this.result = null;
        }

        public PromiseMonitorEvent(T result, Future future) {
            this.result = result;
            this.future = future;
            this.error = null;
        }

        public Throwable getError() {
            return error;
        }

        public T getResult() {
            return result;
        }

        public Future getFuture() {
            return future;
        }
    }
}
