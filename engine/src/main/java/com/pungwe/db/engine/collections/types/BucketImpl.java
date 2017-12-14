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
import com.pungwe.db.core.error.DatabaseRuntimeException;
import com.pungwe.db.core.io.serializers.*;
import com.pungwe.db.core.result.DeletionResult;
import com.pungwe.db.core.result.InsertResult;
import com.pungwe.db.core.result.QueryResult;
import com.pungwe.db.core.result.UpdateResult;
import com.pungwe.db.core.types.Bucket;
import com.pungwe.db.core.types.DBObject;
import com.pungwe.db.core.utils.TypeReference;
import com.pungwe.db.engine.io.BasicRecordFile;
import com.pungwe.db.common.io.RecordFile;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
    private final BucketMetaData metaData;
    private final File directory;
    private final NavigableMap<UUID, RecordFile<DBObject>> data = new ConcurrentSkipListMap<>(UUID::compareTo);
    private final Map<String, LSMTreeIndex> indexes = new LinkedHashMap<>();
    private final Serializer<DBObject> dbObjectSerializer = new DBObjectSerializer();
    private boolean dropped = false;

    BucketImpl(File directory, String name) throws IOException {
        this.directory = directory;
        this.metaData = load(name);
    }

    BucketImpl(File directory, String name, ConcurrentMap<String, Object> config) throws IOException {
        this.directory = directory;
        // This is a new database...
        this.metaData = new BucketMetaData(name, 0, config, new LinkedHashMap<>());
    }

    private BucketMetaData load(String name) throws IOException {
        // Load the meta data...
        BucketMetaData metaData = loadMetaData(name);
        // Fetch all the data files...
        loadDataFiles();
        // Load indexes
        loadIndexes();

        return metaData;
    }

    private BucketMetaData loadMetaData(String name) throws IOException {
        // Find the meta data file.
        File meta = new File(directory, name + "_meta.db");
        boolean exists = meta.exists();
        // If the meta file exists, then open a reader and pull the meta data out of it...
        if (exists) {
            RandomAccessFile raf = new RandomAccessFile(meta, "r");
            return new BucketMetaDataSerializer().deserialize(raf);
        }
        return new BucketMetaData(name, 0, new LinkedHashMap<>(), new LinkedHashMap<>());
    }

    private void loadDataFiles() throws IOException {
        Pattern p = Pattern.compile(metaData.getName() + "_([\\w\\d\\-]+)_data.db");
        String[] files = directory.list((dir, name) -> p.matcher(name).matches());
        if (files == null) {
            return;
        }
        for (String file : files) {
            String id = p.matcher(file).group(1);
            data.put(UUID.fromString(id), new BasicRecordFile<>(new File(directory, file), null));
        }
    }

    private void loadIndexes() throws IOException {
        Map<String, IndexConfig> indexNames = metaData.getIndexConfig();
        for (Map.Entry<String, IndexConfig> e : indexNames.entrySet()) {
            indexes.put(e.getKey(), LSMTreeIndex.getInstance(directory, e.getValue()));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Promise<List<IndexConfig>> indexes() {
        Collection<LSMTreeIndex> indexes = this.indexes.values();
        List<IndexConfig> result = ImmutableList.copyOf(indexes.stream().map(LSMTreeIndex::getConfig)
                .collect(Collectors.toList()));
        Promise.PromiseBuilder<List<IndexConfig>> promise = Promise.build(new TypeReference<List<IndexConfig>>() {});
        promise.keep(result);
        return promise.promise();
    }

    @Override
    public Promise<Void> createIndex(final IndexConfig indexConfig) {
        /*
         * Index is created via a runnable promise and immediately starts building... The index config is checked for
         * validity. If it is not valid (for example there can only be one primary) then the promise will fail. If it's
         * constraints can't be met (i.e. if it's a unique index and records in the bucket have duplicate values), then
         * the promise will fail.
         */
        return Promise.build(new TypeReference<Void>() {}).given(() -> {
            // Ensure that the configuration is not for a primary index...
            if (indexConfig.isPrimary()) {
                throw new DatabaseRuntimeException("You cannot create a primary index...");
            }
            // Create an instance of the index
            LSMTreeIndex index = LSMTreeIndex.getInstance(directory, indexConfig);
            // Stream the index values into the index
            indexes.get("primary").stream().forEach(lsmEntry -> index.put((DBObject)lsmEntry.getValue(), -1));
            return null;
        }).promise();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Promise<Boolean> containsIndex(String name) {
        Promise.PromiseBuilder<Boolean> builder = Promise.build(new TypeReference<Boolean>() {});
        builder.keep(indexes != null && indexes.containsKey(name));
        return builder.promise();
    }

    @Override
    public Promise<IndexConfig> getIndex(String name) {
        Map<String, IndexConfig> indexes = metaData.getIndexConfig();
        Promise.PromiseBuilder<IndexConfig> b = Promise.build(new TypeReference<IndexConfig>() {});
        b.keep(indexes.get(name));
        return b.promise();
    }

    @Override
    public long size() {
        return metaData.getSize();
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
