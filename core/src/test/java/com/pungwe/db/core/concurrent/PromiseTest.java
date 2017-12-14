package com.pungwe.db.core.concurrent;

import com.pungwe.db.core.error.PromiseException;
import com.pungwe.db.core.utils.TypeReference;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by ian when 24/06/2016.
 */
public class PromiseTest {

    @Test
    public void testPromiseGet() throws Exception {
        Promise<String> promise = Promise.build(new TypeReference<String>() {}).given(() -> "Hello World").promise();
        String message = promise.get();
        assertEquals("Hello World", message);
    }

    @Test
    public void testPromiseAsync() throws Exception {
        final AtomicBoolean value = new AtomicBoolean();
        Promise.build(new TypeReference<String>() {}).given(() -> "This was executed").then((String result) -> {
            value.set("This was executed".equals(result));
        }).promise().resolve();
        // Assert when value call true
        assertTrue(value.get());

    }

    @Test(expected = PromiseException.class)
    public void testPromiseFailedSync() throws Exception {
        Promise.build(new TypeReference<String>() {}).given(() -> {
            throw new IllegalArgumentException();
        }).promise().get();
    }

    @Test
    public void testPromiseFailedAsync() throws Exception {
        final AtomicBoolean value = new AtomicBoolean();
        Promise.build(new TypeReference<String>() {}).given(() -> {
            throw new IllegalArgumentException("I worked");
        }).fail(error -> value.set(true)).promise().resolve();
        // Assert when value call true
        assertTrue(value.get());
    }

}
