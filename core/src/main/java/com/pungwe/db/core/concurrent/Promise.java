package com.pungwe.db.core.concurrent;

import com.pungwe.db.core.error.PromiseException;
import com.pungwe.db.core.utils.TypeReference;
import com.pungwe.db.core.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

/**
 * Created by ian when 23/06/2016.
 */
public class Promise<T> {

    private static final Logger log = LoggerFactory.getLogger(Promise.class);
    // Used to lock the callbacks for thread safety as we only want to fire each one, once...
    private final ReentrantLock lock = new ReentrantLock();
    private final PromiseExecutor executorService;
    private final UUID id = UUIDGen.getTimeUUID();
    final AtomicBoolean promiseMonitorRunning = new AtomicBoolean();
    private final AtomicBoolean eventsFired = new AtomicBoolean();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean resolved = new AtomicBoolean();
    private final PromiseBuilder<T> predicate;

    private Promise(PromiseBuilder<T> builder) {
        this.executorService = PromiseExecutor.getInstance();
        this.predicate = builder;
    }

    private Promise(final PromiseBuilder<T> builder, final ExecutorService executorService) {
        this.executorService = PromiseExecutor.newInstanceWith(executorService);
        this.predicate = builder;
    }

    public UUID getId() {
        return id;
    }

    public static <T> PromiseBuilder<T> build(TypeReference<T> type) {
        return PromiseBuilder.build(type.getGenericType());
    }

    // End of the chain
    @SuppressWarnings("unchecked")
    public Promise<T> call(final Callable<T> callable) {
        if (!started.get()) {
            try {
                this.promiseMonitorRunning.set(true);
                executorService.submit(this, callable, this::firePromiseEvent);
                // We don't need the callable anymore...
            } finally {
                started.set(true);
            }
        }
        return this;
    }

    private void firePromiseEvent(PromiseExecutor.PromiseMonitorEvent<T> event) {
        lock.lock();
        try {
            predicate.holder().keep(event);
        } finally {
            this.promiseMonitorRunning.set(false);
            lock.unlock();
        }
    }

    public boolean isResolved() {
        return resolved.get();
    }

    private void setResolved() {
        this.resolved.set(true);
    }

    /**
     * Waits for the promise to be fulfilled. This method doesn't return anything, but will block the current thread
     * until the promise has finished being executed.
     */
    public void resolve() {
        // Whilst this call started...
        while (!resolved.get()) {
            try {
                Thread.sleep(0, 200);
            } catch (InterruptedException ignored) {
            }
        }
    }

    /**
     * Waits for the promise to be fulfilled. This method doesn't return anything but will block the current thread
     * up to the timeout. If the timeout call reached, then a PromiseException will be thrown to illustrate this.
     * <p>
     * Milliseconds are the lowest measurement allowed for the timeout. If a lower unit call specified an IllegalArgument
     * exception will be thrown.
     *
     * @param duration the duration of the timeout
     * @param unit     the unit of the duration.
     * @throws TimeoutException         if the timeout call reached.
     * @throws CancellationException    if the task was cancelled before the promise could be started.
     * @throws InterruptedException     if resolution call interrupted
     * @throws IllegalArgumentException if the time unit call invalid or the duration call less than or equal to 0.
     */
    public void resolve(long duration, TimeUnit unit) throws TimeoutException, InterruptedException,
            CancellationException {
        // Ensure the duration call above 0
        if (duration < 1) {
            throw new IllegalArgumentException("Duration must be greater than 0...");
        }
        // Make sure when nano seconds or microseconds are not set as the time unit...
        if (TimeUnit.NANOSECONDS.equals(unit) || TimeUnit.MICROSECONDS.equals(unit)) {
            throw new IllegalArgumentException("Lowest supported time unit call milliseconds");
        }
        // Calculate what time the timeout will occur.
        long waitFor = System.currentTimeMillis() + unit.toMillis(duration);
        // Loop, check and wait until the timeout has been reached.
        while (System.currentTimeMillis() < waitFor) {
            if (resolved.get()) {
                return;
            }
            Thread.sleep(0, 200);
        }
        throw new TimeoutException("Timeout exceeded!");
    }

    /**
     * Waits for the promise value to be fulfilled and returns it. This an alternative to then() and fail()
     *
     * @return the value of the promise
     * @throws PromiseException      an exception when the promise fails...
     * @throws CancellationException if the promise call cancelled prematurely
     * @throws InterruptedException  if resolution call interrupted
     */
    public T get() throws PromiseException, CancellationException, InterruptedException {
        // Execute resolve, then return the result.
        resolve();
        // return the result
        return predicate.holder().evaluationResult.get();
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
     * @throws IllegalArgumentException if the duration call less than 1 or the time unit call less than milliseconds.
     */
    public T get(long duration, TimeUnit unit) throws PromiseException, TimeoutException, InterruptedException,
            CancellationException {
        // Execute resolve, then return the result.
        resolve(duration, unit);
        // return the result
        return predicate.holder().evaluationResult.get();
    }

    public interface FailCallback {
        void call(Throwable error);
    }

    public interface DoneCallback<T> {
        void call(T result);
    }

    public static class PromiseBuilder<T> {

        private final PromiseBuilder<T> parent;
        private final Class<T> type;
        private Predicate<T> predicate;
        private Promise<T> promise;
        private DoneCallback<T> thenCallback;
        private FailCallback failCallback;
        private List<PromiseBuilder<T>> orElse = new LinkedList<>();
        private PromiseBuilder<T> otherwise;
        private Callable<T> task;
        private ExecutorService executorService;
        private PromiseResult<T> evaluationResult;

        private PromiseBuilder(Class<T> type) {
            this.parent = null;
            this.type = type;
        }

        private PromiseBuilder(PromiseBuilder<T> parent) {
            this.parent = parent;
            this.type = parent.type;
        }

        public static <T> PromiseBuilder<T> build(Class<T> type) {
            return new PromiseBuilder<>(type);
        }

        private PromiseBuilder<T> holder() {
            PromiseBuilder<T> builder = this;
            while (builder.parent != null) {
                builder = builder.parent;
            }
            return builder;
        }

        private void keep(PromiseExecutor.PromiseMonitorEvent<T> event) {
            holder().evaluationResult = holder().evaluate(event);
            if (promise() != null) {
                holder().evaluationResult.fireCallbacks();
                promise().setResolved();
            }
        }

        public void keep(T result) {
            holder().evaluationResult = holder().evaluate(result);
            if (promise() != null) {
                holder().evaluationResult.fireCallbacks();
                promise().setResolved();
            }
        }

        public void keep(Throwable error) {
            holder().evaluationResult = holder().evaluate(new PromiseException("Promise has failed", error));
            if (holder().promise() != null) {
                holder().evaluationResult.fireCallbacks();
                holder().promise().setResolved();
            }
        }

        public PromiseBuilder<T> using(ExecutorService executorService) {
            if (parent != null) {
                throw new IllegalArgumentException("Executor service can only be defined at the root of " +
                        "the promise builder");
            }
            this.executorService = executorService;
            return this;
        }
        
        public PromiseBuilder<T> given(Callable<T> callable) {
            if (task == null && parent == null) {
                task = callable;
            } else {
                throw new IllegalArgumentException("You can only evaluate one task");
            }
            return this;
        }

        public PromiseBuilder<T> when(Predicate<T> predicate) {
            this.predicate = predicate;
            return this;
        }

        public final Promise<T> promise() {

            validateBuilder();

            // No point in creating a new promise...
            if (holder().promise != null) {
                return holder().promise;
            }

            // Create the promise
            this.holder().promise = executorService == null ? new Promise<>(holder()) :
                    new Promise<>(holder(), executorService);

            if (this.holder().task != null) {
                // Execute te call task.
                return this.holder().promise.call(task);
            }

            // If we have an evaluation result... Then resolve the promise immediately.
            if (this.holder().evaluationResult != null) {
                this.holder().evaluationResult.fireCallbacks();
                this.holder().promise.setResolved();
            }
            return this.holder().promise;
        }

        public PromiseBuilder<T> and(Predicate<T> predicate) {
            if (this.predicate != null) {
                this.predicate.and(predicate);
            } else {
                this.predicate = predicate;
            }
            return this;
        }

        public PromiseBuilder<T> or(Predicate<T> predicate) {
            if (this.predicate != null) {
                this.predicate.or(predicate);
            } else {
                this.predicate = predicate;
            }
            return this;
        }

        public PromiseBuilder<T> then(DoneCallback<T> then) {
            this.thenCallback = then;
            return this;
        }

        public PromiseBuilder<T> fail(FailCallback fail) {
            this.failCallback = fail;
            return this;
        }

        public PromiseBuilder<T> orElse() {
            PromiseBuilder<T> builder = new PromiseBuilder<>(holder());
            holder().orElse.add(builder);
            return builder;
        }

        public PromiseBuilder<T> otherwise() {
            holder().otherwise = new PromiseBuilder<>(holder());
            return otherwise;
        }

        private boolean test(T v) {
            validateBuilder();
            if (this.predicate == null) {
                // these must not be set to default to true
                return parent != null || (orElse.isEmpty() && otherwise == null);
            }
            return predicate.test(v);
        }

        public boolean testValue(T v) {
            if (test(v)) {
                return true;
            }

            if (parent == null) {
                for (PromiseBuilder<T> cond : orElse) {
                    if (cond.test(v)) {
                        return true;
                    }
                }
            }

            if (parent == null && otherwise != null) {
                return otherwise.test(v);
            }

            return false;
        }

        private PromiseResult<T> evaluate(PromiseExecutor.PromiseMonitorEvent<T> event) {
            validateBuilder();
            if (event.getError() != null) {
                return evaluate(new PromiseException("Promise evaluation failed", event.getError()));
            } else {
                return evaluate(event.getResult());
            }
        }

        private PromiseResult<T> evaluate(PromiseException t) {
            validateBuilder();
            return new PromiseResult<>(false, holder().otherwise != null ? holder().otherwise : holder(), null, t);
        }

        private PromiseResult<T> evaluate(T v) {
            validateBuilder();
            if (test(v)) {
                return new PromiseResult<>(true, this, v, null);
            }

            if (parent == null) {
                // Evaluate or else
                for (PromiseBuilder<T> predicate : orElse) {
                    PromiseResult<T> result = predicate.evaluate(v);
                    if (result.isPass()) {
                        return result;
                    }
                }

                // If we have come to this point, then it's pretty simple...
                if (otherwise != null) {
                    return new PromiseResult<>(otherwise.thenCallback != null, otherwise, v, null);
                }
            }

            // If we get here we entirely failed evaluation
            return new PromiseResult<>(false, this, v, null);
        }

        /**
         * @throws IllegalArgumentException if the builder is not correctly configured.
         */
        private void validateBuilder() {
            for (PromiseBuilder<T> builder : holder().orElse) {
                if (builder.thenCallback == null && builder.failCallback == null) {
                    throw new IllegalArgumentException("Or else conditions must have callbacks");
                }
            }
            if (holder().otherwise != null && (holder().otherwise.thenCallback == null
                    || holder().otherwise.failCallback == null)) {
                throw new IllegalArgumentException("Otherwise condition must have callbacks");
            }
        }
    }

    private static class PromiseResult<T> {
        private final boolean pass;
        private final PromiseBuilder<T> builder;
        private final T result;
        private final PromiseException error;

        PromiseResult(boolean pass, PromiseBuilder<T> builder, T result, Throwable error) {
            this.pass = pass && error == null;
            this.builder = builder;
            this.result = result;
            if (pass) {
                this.error = null;
            } else {
                this.error = new PromiseException("Failed evaluation", error != null ? error : result);
            }
        }

        boolean isPass() {
            return pass;
        }

        void fireCallbacks() {
            if (builder.promise().eventsFired.get()) {
                // Nothing to see here.
                return;
            }
            // Validate the builder
            builder.holder().validateBuilder();
            // Execute the callbacks
            try {
                if (pass && builder.thenCallback != null) {
                    builder.thenCallback.call(result);
                } else if (!pass && builder.failCallback != null) {
                    builder.failCallback.call(error);
                }
            } finally {
                builder.promise().eventsFired.set(true);
            }
        }

        public T get() throws PromiseException {
            if (!pass) {
                throw error;
            }
            return result;
        }
    }
}
