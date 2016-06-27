package com.pungwe.db.core.concurrent;

import com.pungwe.db.core.error.PromiseException;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by ian on 24/06/2016.
 */
public class PromiseTest {

    @Test
    public void testPromiseGet() throws Exception {

        Promise<String> promise = Promise.when(() -> {
            return "Hello World";
        });

        String message = promise.get();
        assertEquals("Hello World", message);
    }

    @Test
    public void testPromiseAsync() throws Exception {
        final AtomicBoolean value = new AtomicBoolean();
        Promise.when(() -> "This was executed").then((String result) -> {
            System.out.println("Result: " + result);
            value.set(result != null && result.equals("This was executed"));
        }).resolve();
        // Assert that value is true
        assertTrue(value.get());
    }

    @Test(expected = PromiseException.class)
    public void testPromiseFailedSync() throws Exception {
        Promise.when(() -> {
            throw new IllegalArgumentException();
        }).get();
    }

    @Test
    public void testPromiseFailedAsync() throws Exception {
        final AtomicBoolean value = new AtomicBoolean();
        Promise.when(() -> {
            throw new IllegalArgumentException("I worked");
        }).fail(error -> value.set(true)).resolve();
        // Assert that value is true
        assertTrue(value.get());
    }

    @Test
    public void testBigThreads() throws Exception {
        for (int i = 0; i < 1000; i++) {
            final int count = i;
            Promise.when(() -> {
                Promise.when(() -> {
                    Promise.when(() -> {
                        System.out.println("Done: " + count);
                        return null;
                    }).get(30, TimeUnit.SECONDS);
                    return null;
                }).get(30, TimeUnit.SECONDS);
                return null;
            }).get(60, TimeUnit.SECONDS);
        }
    }
}
