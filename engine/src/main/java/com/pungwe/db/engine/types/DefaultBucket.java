package com.pungwe.db.engine.types;

import com.pungwe.db.core.command.Query;
import com.pungwe.db.core.concurrent.Promise;
import com.pungwe.db.core.error.IndexAlreadyExistsException;
import com.pungwe.db.core.result.DeletionResult;
import com.pungwe.db.core.result.InsertResult;
import com.pungwe.db.core.result.QueryResult;
import com.pungwe.db.core.result.UpdateResult;
import com.pungwe.db.core.types.Bucket;
import com.pungwe.db.core.types.DBObject;
import com.pungwe.db.engine.io.store.Store;
import com.pungwe.db.engine.types.factory.IndexFactory;
import com.pungwe.db.engine.types.meta.Index;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by ian on 24/06/2016.
 */
public class DefaultBucket implements Bucket<DBObject<String, Object>> {

    protected final BucketMetaData<Index> bucketMetaData;
    protected final Store store;

    public DefaultBucket(BucketMetaData<Index> bucketMetaData, Store store) {
        this.bucketMetaData = bucketMetaData;
        this.store = store;
    }

    @Override
    public Promise<List<String>> indexes() {
        return Promise.when(() -> {
            synchronized (bucketMetaData) {
                return bucketMetaData.getIndexes().keySet().stream().collect(Collectors.toList());
            }
        });
    }

    @Override
    public Promise<Boolean> containsIndex(String name) {
        return Promise.when(() -> {
            synchronized (bucketMetaData) {
                return bucketMetaData.getIndexes().containsKey(name);
            }
        });
    }

    @Override
    public Promise<Void> createIndex(String name, Map<String, Object> options) {
        return Promise.when(() -> {
            if (bucketMetaData.getIndexes().containsKey(name)) {
                throw new IndexAlreadyExistsException("Index already exists!");
            }
            Index index = IndexFactory.createIndex(store, name, options);
            this.bucketMetaData.getIndexes().put(name, index);
            return null;
        });
    }

    @Override
    public Promise<Map<String, Object>> getIndex(String name) {
        return Promise.when(() -> {
            // Check to see if the index exists...
            if (!this.bucketMetaData.getIndexes().containsKey(name)) {
                return null;
            }
            // If the index exists, return it's config
            Index index = this.bucketMetaData.getIndexes().get(name);
            return index.getConfig();
        });
    }

    @Override
    public long size() {
        return bucketMetaData.getSize();
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
