/*
 * Copyright (C) 2016 Ian Michell.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pungwe.db.engine.collections.types;

import com.pungwe.db.core.command.Query;
import com.pungwe.db.core.concurrent.Promise;
import com.pungwe.db.core.result.DeletionResult;
import com.pungwe.db.core.result.InsertResult;
import com.pungwe.db.core.result.QueryResult;
import com.pungwe.db.core.result.UpdateResult;
import com.pungwe.db.core.types.Bucket;

import java.util.List;
import java.util.Map;

/**
 * <p>This class manages a local database instance within the default storage engine. It's configured with a
 * <code>RecordDirectory</code> and corresponding options within it's meta data.</p>
 * <p>A bucket instance provides an access point for inserting, updating and finding records.</p>
 *
 * <p>
 *     <code>
 *          Result result = bucket.find(Query.select("*").where("key").equalTo("value").and("key2").in(1,2,3).get();
 *     </code>
 * </p>
 *
 * <p>All operational methods return a <code>Promise</code> object allowing for asynchronous processing.</p>
 * <p>
 *     <code>
 *          bucket.find(Query.select("*").where("key").equalTo("value")).then((result) -> {
 *              ...
 *          }).resolve();
 *     </code>
 * </p>
 */
public class BucketImpl<T> implements Bucket<T> {

    protected BucketImpl(String name, Map<String, Object> metaData) {

    }

    public BucketImpl<T> getInstance(String name) {
        return null;
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
    public Promise<T> findOne(Object id) {
        return null;
    }

    @Override
    public Promise<UpdateResult> save(T... object) {
        return null;
    }

    @Override
    public Promise<UpdateResult> update(Query query) {
        return null;
    }

    @Override
    public Promise<InsertResult> insert(T... object) {
        return null;
    }

    @Override
    public Promise<DeletionResult> remove(Object... object) {
        return null;
    }
}
