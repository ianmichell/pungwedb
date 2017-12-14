package com.pungwe.db.core.types;

import com.google.common.collect.ImmutableList;
import com.pungwe.db.core.command.Query;
import com.pungwe.db.core.concurrent.Promise;
import com.pungwe.db.core.io.serializers.MapSerializer;
import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.io.serializers.StringSerializer;
import com.pungwe.db.core.result.DeletionResult;
import com.pungwe.db.core.result.InsertResult;
import com.pungwe.db.core.result.QueryResult;
import com.pungwe.db.core.result.UpdateResult;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Created by ian on 18/06/2016.
 */
public interface Bucket<T> {

    /**
     * Returns a list of index names
     *
     * @return a promise with a list of index names on completion
     */
    Promise<List<IndexConfig>> indexes();

    /**
     * Creates an index with the given name if it doesn't exist. If not it will simply return
     *
     * @param config the options for the index.
     */
    Promise<Void> createIndex(IndexConfig config);


    /**
     * Checks to see if the index with a given name exists
     *
     * @param name the name of the index to be checked
     *
     * @return a boolean promise with either true if the index exsits, or false if it doesn't.
     */
    Promise<Boolean> containsIndex(String name);

    /**
     * Return an index definition if it exists, null if not
     *
     * @param name the name of the index to be fetched
     *
     * @return a promise for DBObject containing the index configuration for the given name.
     */
    Promise<IndexConfig> getIndex(String name);

    /**
     * Returns the size of this bucket
     *
     * @return the size of this bucket
     */
    long size();

    /**
     * Drops the bucket and returns a promise to monitor for success or failure
     *
     * @return the promise object containing the result of the execution
     */
    Promise<Void> drop();

    /**
     * Execute a find query and return a promise
     *
     * @param query the query to execute
     *
     * @return the promise object containing the result of the execution
     */
    Promise<QueryResult> find(Query query);

    /**
     * Find the relevant object by it's id
     *
     * @param id the id of the object to be found
     *
     * @return the found object or null if none.
     */
    Promise<T> findOne(Object id);

    /**
     * Execute a save operation (insert or update) and return a promise
     *
     * @param object the object to update
     *
     * @return the promise object containing the result of the execution
     */
    Promise<UpdateResult> save(T... object);

    /**
     * Execute an update with the given query.
     *
     * @param query the update query
     *
     * @return the promise object containing the result of the execution
     */
    Promise<UpdateResult> update(Query query);

    /**
     * Execute an insert for the given objects in this bucket and return a promise
     *
     * @param object the object(s) to be inserted
     *
     * @return the promise object containing the result of the execution
     */
    Promise<InsertResult> insert(T... object);

    /**
     * Execute a deletion and return a promise
     *
     * @param object the object to delete (ID or Record)
     *
     * @return the promise object containing the result of the execution
     */
    Promise<DeletionResult> remove(Object... object);

    /**
     * Stores index configuration.
     *
     * primary is true if the index is a primary index
     * unique is true if primary is true, or a unique secondary index
     * memoryTreeSize is the maximum number of elements that can be contained in memory before being flushed to disk...
     *
     */
    class IndexConfig {
        private final String name;
        private final List<IndexField> fields;
        private final boolean primary;
        private final boolean unique;
        private final int memoryTreeSize;

        public IndexConfig(String name, boolean primary, boolean unique, int memoryTreeSize, List<IndexField> fields) {
            this.name = name;
            this.primary = primary;
            this.unique = unique;
            this.memoryTreeSize = memoryTreeSize;
            // Ensure we don't have duplicate copies of these fields...
            this.fields = ImmutableList.copyOf(fields.stream().distinct().collect(Collectors.toList()));
        }

        public String getName() {
            return name;
        }

        public List<IndexField> getFields() {
            return fields;
        }

        public boolean isPrimary() {
            return primary;
        }

        public boolean isUnique() {
            return unique;
        }

        public int getMemoryTreeSize() {
            return memoryTreeSize;
        }

        public boolean hasFields(Collection<String> fields) {
            List<String> fieldNames = this.fields.stream().map(IndexField::getName).collect(Collectors.toList());
            return fieldNames.containsAll(fields);
        }
    }

    class IndexField {
        private final String name;
        private boolean descending;

        public IndexField(String name, boolean descending) {
            this.name = name;
            this.descending = descending;
        }

        public String getName() {
            return name;
        }

        public boolean isDescending() {
            return descending;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IndexField that = (IndexField) o;

            return name.equals(that.name);

        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

    class BucketMetaData {

        private String name;
        private AtomicLong size = new AtomicLong();
        private Map<String, Object> config;
        private Map<String, IndexConfig> indexConfig;

        public BucketMetaData(String name, long size, Map<String, Object> config,
                              Map<String, IndexConfig> indexConfig) {
            this.name = name;
            this.size.set(size);
            this.config = config;
            this.indexConfig = indexConfig;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Map<String, Object> getConfig() {
            return config;
        }

        public void setConfig(Map<String, Object> config) {
            this.config = config;
        }

        public Map<String, IndexConfig> getIndexConfig() {
            return indexConfig;
        }

        public void setIndexConfig(Map<String, IndexConfig> indexConfig) {
            this.indexConfig = indexConfig;
        }

        public void setSize(long size) {
            this.size.set(size);
        }

        public long getSize() {
            return this.size.get();
        }

        public long getAndIncrement() {
            return this.size.getAndIncrement();
        }

        public long getAndDecrement() {
            return this.size.getAndDecrement();
        }

        public long getAndAdd(long add) {
            return this.size.getAndAdd(add);
        }
    }

    class BucketMetaDataSerializer implements Serializer<BucketMetaData> {

        private final static MapSerializer<String, Object> configSerializer = new MapSerializer<>(new StringSerializer(),
                new ObjectSerializer());
        private final static MapSerializer<String, IndexConfig> indexSerializer = new MapSerializer<>(
                new StringSerializer(), new IndexConfigSerializer());

        @Override
        public void serialize(DataOutput out, BucketMetaData value) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream d = new DataOutputStream(bytes);
            d.writeUTF(value.getName());
            d.writeLong(value.getSize());
            configSerializer.serialize(d, value.getConfig());
            indexSerializer.serialize(d, value.getIndexConfig());
            out.write(bytes.toByteArray());
        }

        @Override
        public BucketMetaData deserialize(DataInput in) throws IOException {
            String name = in.readUTF();
            long size = in.readLong();
            Map<String, Object> config = configSerializer.deserialize(in);
            Map<String, IndexConfig> indexes = indexSerializer.deserialize(in);
            return new BucketMetaData(name, size, config, indexes);
        }

        @Override
        public String getKey() {
            return "BMD";
        }
    }

    class IndexConfigSerializer implements Serializer<IndexConfig> {

        @Override
        public void serialize(DataOutput out, IndexConfig value) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream d = new DataOutputStream(bytes);
            d.writeUTF(value.getName());
            d.writeInt(value.getMemoryTreeSize());
            d.writeBoolean(value.isPrimary());
            d.writeBoolean(value.isUnique());
            d.writeInt(value.getFields().size());
            for (IndexField field : value.getFields()) {
                d.writeUTF(field.getName());
                d.writeBoolean(field.isDescending());
            }
        }

        @Override
        public IndexConfig deserialize(DataInput in) throws IOException {
            String name = in.readUTF();
            int memoryTreeSize = in.readInt();
            boolean primary = in.readBoolean();
            boolean unique = in.readBoolean();
            int fieldSize = in.readInt();
            List<IndexField> fields = new ArrayList<>(fieldSize);
            for (int i = 0; i < fieldSize; i++) {
                String fieldName = in.readUTF();
                boolean descending = in.readBoolean();
                fields.add(new IndexField(fieldName, descending));
            }
            return new IndexConfig(name, primary, primary ? true : unique, memoryTreeSize, fields);
        }

        @Override
        public String getKey() {
            return "IDX:CFG";
        }
    }
}
