package com.pungwe.db.engine.types;

import com.pungwe.db.core.concurrent.Promise;
import com.pungwe.db.core.types.Bucket;
import com.pungwe.db.core.types.Database;

import java.util.Map;
import java.util.Set;

/**
 * Created by ian on 24/06/2016.
 */
public class DatabaseImpl implements Database {

    @Override
    public String getName() {
        return null;
    }

    @Override
    public Promise<Set<String>> getBucketNames() {
        return Promise.when(() -> {
            return null;
        });
    }

    @Override
    public Promise<Bucket<?>> getBucket(String name) {
        return Promise.when(() -> {
            return null;
        });
    }

    @Override
    public Promise<Bucket<?>> createBucket(String name, Map<String, Object> options) {
        return Promise.when(() -> {
            return null;
        });
    }

    @Override
    public Promise<Void> dropBucket(String name) {
        return Promise.when(() -> {
            return null;
        });
    }
}
