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

import com.google.common.collect.ImmutableList;
import com.pungwe.db.core.command.Query;
import com.pungwe.db.core.concurrent.Promise;
import com.pungwe.db.core.io.serializers.*;
import com.pungwe.db.core.result.DeletionResult;
import com.pungwe.db.core.result.InsertResult;
import com.pungwe.db.core.result.QueryResult;
import com.pungwe.db.core.result.UpdateResult;
import com.pungwe.db.core.types.Bucket;
import com.pungwe.db.core.types.DBObject;
import com.pungwe.db.core.utils.TypeReference;
import com.pungwe.db.engine.io.BasicRecordFile;
import com.pungwe.db.engine.io.RecordFile;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Pattern;

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
public class BucketImpl implements Bucket<DBObject> {

    private static final String PRIMARY_INDEX_NAME = "primary";
    private final ConcurrentMap<String, Object> metaData;
    private final File directory;
    private final String name;
    private final NavigableMap<UUID, RecordFile<DBObject>> data = new ConcurrentSkipListMap<>(UUID::compareTo);
    private final Map<String, LSMTreeIndex> indexes = new LinkedHashMap<>();
    private final Serializer<DBObject> dbObjectSerializer = new DBObjectSerializer();

    BucketImpl(File directory, String name) throws IOException {
        this.directory = directory;
        this.name = name;
        this.metaData = new ConcurrentSkipListMap<>();
    }

    BucketImpl(File directory, String name, ConcurrentMap<String, Object> metaData) throws IOException {
        this.directory = directory;
        this.name = name;
        // This is a new database...
        this.metaData = metaData;
    }

    private void load() throws IOException {
        // Load the meta data...
        loadMetaData();
        // Fetch all the data files...
        loadDataFiles();
        // Load indexes
    }

    private void loadMetaData() throws IOException {
        // Find the meta data file.
        File meta = new File(directory, name + "_meta.db");
        boolean exists = meta.exists();
        // If the meta file exists, then open a reader and pull the meta data out of it...
        if (exists) {
            RandomAccessFile raf = new RandomAccessFile(meta, "r");
            Map<String, Object> read = new MapSerializer<>(new StringSerializer(), new ObjectSerializer())
                    .deserialize(raf);
            // Load the emta data...
            metaData.putAll(read);
        }
    }

    private void loadDataFiles() throws IOException {
        Pattern p = Pattern.compile(name + "_([\\w\\d\\-]+)_data.db");
        String[] files = directory.list((dir, name) -> p.matcher(name).matches());
        if (files == null) {
            return;
        }
        for (String file : files) {
            String id = p.matcher(file).group(1);
            data.put(UUID.fromString(id), new BasicRecordFile<>(new File(directory, file), null));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Promise<List<String>> indexes() {
        Collection<String> indexes = this.indexes.keySet();
        List<String> result = ImmutableList.copyOf(indexes);
        Promise.PromiseBuilder<List<String>> promise = Promise.build(new TypeReference<List<String>>() {});
        promise.keep(result);
        return promise.promise();
    }

    @Override
    public Promise<Void> createIndex(String name, Map<String, Object> options) {
        // LSMTreeIndex builder should create and manage an index based on the configuration options...
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Promise<Boolean> containsIndex(String name) {
        Collection<String> indexes = (Collection<String>)metaData.get("indexes");
        Promise.PromiseBuilder<Boolean> builder = Promise.build(new TypeReference<Boolean>() {});
        builder.keep(indexes != null && indexes.contains(name));
        return builder.promise();
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
    public Promise<DBObject> findOne(Object id) {
        return null;
    }

    @Override
    public Promise<UpdateResult> save(DBObject... object) {
        return null;
    }

    @Override
    public Promise<UpdateResult> update(Query query) {
        return null;
    }

    @Override
    public Promise<InsertResult> insert(DBObject... object) {
        return null;
    }

    @Override
    public Promise<DeletionResult> remove(Object... object) {
        return null;
    }
}
