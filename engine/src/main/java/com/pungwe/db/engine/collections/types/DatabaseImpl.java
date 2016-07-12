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

import com.pungwe.db.core.concurrent.Promise;
import com.pungwe.db.core.types.Bucket;
import com.pungwe.db.core.types.Database;

import java.util.Map;
import java.util.Set;

/**
 * Created by ian on 12/07/2016.
 */
public class DatabaseImpl implements Database {

    @Override
    public String getName() {
        return null;
    }

    @Override
    public Promise<Set<String>> getBucketNames() {
        return null;
    }

    @Override
    public Promise<Bucket<?>> getBucket(String name) {
        return null;
    }

    @Override
    public Promise<Bucket<?>> createBucket(String name, Map<String, Object> options) {
        return null;
    }

    @Override
    public Promise<Void> dropBucket(String name) {
        return null;
    }
}
