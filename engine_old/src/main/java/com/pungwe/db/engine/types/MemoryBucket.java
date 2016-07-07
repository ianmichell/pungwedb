package com.pungwe.db.engine.types;

import com.pungwe.db.core.command.Query;
import com.pungwe.db.core.concurrent.Promise;
import com.pungwe.db.core.result.DeletionResult;
import com.pungwe.db.core.result.InsertResult;
import com.pungwe.db.core.result.QueryResult;
import com.pungwe.db.core.result.UpdateResult;
import com.pungwe.db.core.types.Bucket;
import com.pungwe.db.core.types.DBObject;
import com.pungwe.db.engine.io.store.Store;

import java.lang.annotation.Documented;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by ian on 24/06/2016.
 */
public class MemoryBucket implements Bucket<DBObject<String, Object>> {

    public MemoryBucket() {

    }

    @Override
    public Promise<List<String>> indexes() {
        return null;
    }

    @Override
    public Promise<Void> createIndex(String name, Map<String, Object> options) {
        return null;
    }

    @Override
    public Promise<Boolean> containsIndex(String name) {
        return null;
    }

    @Override
    public Promise<Map<String, Object>> getIndex(String name) {
        return null;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public Promise<Void> drop() {
        return null;
    }

    @Override
    public Promise<QueryResult> find(Query query) {
        return null;
    }

    @Override
    public Promise<DBObject<String, Object>> findOne(Object id) {
        return null;
    }

    @Override
    public Promise<UpdateResult> save(DBObject<String, Object>... object) {
        return null;
    }

    @Override
    public Promise<UpdateResult> update(Query query) {
        return null;
    }

    @Override
    public Promise<InsertResult> insert(DBObject<String, Object>... object) {
        return null;
    }

    @Override
    public Promise<DeletionResult> remove(Object... object) {
        return null;
    }
}
