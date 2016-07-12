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
 * Created by ian on 12/07/2016.
 */
public class BucketImpl<T> implements Bucket<T> {

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
