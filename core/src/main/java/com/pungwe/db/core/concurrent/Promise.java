package com.pungwe.db.core.concurrent;

import com.pungwe.db.core.error.PromiseException;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by ian on 23/06/2016.
 */
public class Promise<T> {

    private Future<T> future;
    private FailCallback failCallback;
    private DoneCallback<T> doneCallback;
    private Callable<T> callable;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private Promise() {
    }

    public static <T> Promise<T> when(final Callable<T> callable) {
        final Promise<T> promise = new Promise<T>();
        promise.callable = () -> {
            try {
                T v = callable.call();
                if (promise.doneCallback != null) {
                    promise.doneCallback.call(v);
                }
                return v;
            } catch (Exception ex) {
                if (promise.failCallback != null) {
                    promise.failCallback.call(new PromiseException(ex));
                }
                throw ex; // throw the exception anyway
            }
        };
        return promise;
    }

    public Promise<T> then(DoneCallback<T> callback) {
        this.doneCallback = callback;
        return this;
    }

    public Promise<T> fail(FailCallback callback) {
        this.failCallback = callback;
        return this;
    }

    public Promise<T> execute() {
        this.future = PromiseExecutor.getInstance().submit(callable);
        started.set(true);
        return this;
    }

    /**
     * Waits for the promise value to be fulfilled and returns it. This an alternative to done() and fail()
     *
     * @return the value of the promise
     * @throws PromiseException an exception when the promise fails...
     */
    public T get() throws PromiseException, CancellationException, InterruptedException {
        if (!started.get()) {
            execute();
        }
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw new PromiseException(e);
        }
    }

    public void resolve() {
        if (!started.get()) {
            execute();
        }
        while (!future.isDone()) {
            try {
                Thread.sleep(0, 200);
            } catch (InterruptedException ex) {
            }
        }
    }

    public void resolve(long timeout, TimeUnit unit) throws PromiseException {

    }

    public T get(long timeout, TimeUnit unit) throws PromiseException, TimeoutException, InterruptedException,
            CancellationException {
        try {
            if (!started.get()) {
                execute();
            }
            return future.get(timeout, unit);
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
