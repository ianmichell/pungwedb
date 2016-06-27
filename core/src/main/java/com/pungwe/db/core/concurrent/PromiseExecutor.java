package com.pungwe.db.core.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

final class PromiseExecutor {

    private static PromiseExecutor INSTANCE;

    private final ExecutorService executorService;

    private PromiseExecutor() {
        // We want a work stealing pool to take full advantage of our hardware
        this.executorService = Executors.newWorkStealingPool();
    }

    // No need to make this accessible outside the package
    <T> Future<T> submit(Callable<T> callable) {
        return executorService.submit(callable);
    }

    public static PromiseExecutor getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new PromiseExecutor();
        }
        return INSTANCE;
    }
}
