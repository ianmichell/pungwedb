package com.pungwe.db.core.types;

import com.pungwe.db.core.command.Query;
import com.pungwe.db.core.concurrent.Promise;
import com.pungwe.db.core.result.DeletionResult;
import com.pungwe.db.core.result.InsertResult;
import com.pungwe.db.core.result.QueryResult;
import com.pungwe.db.core.result.UpdateResult;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ian on 18/06/2016.
 */
public interface Bucket<T> {

    /**
     * Returns a list of index names
     *
     * @return a promise with a list of index names on completion
     */
    Promise<List<String>> indexes();

    /**
     * Creates an index with the given name if it doesn't exist. If not it will simply return
     *
     * @param options the options for the index.
     */
    Promise<Void> createIndex(String name, Map<String, Object> options);

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
    Promise<Map<String, Object>> getIndex(String name);

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

    class BucketMetaData<IDX> {

        private String name;
        private AtomicLong size = new AtomicLong();
        private Map<String, Object> config;
        private Map<String, IDX> indexes;

        public BucketMetaData(String name, long size, Map<String, Object> config,
                              Map<String, IDX> indexes) {
            this.name = name;
            this.size.set(size);
            this.config = config;
            this.indexes = indexes;
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

        public Map<String, IDX> getIndexes() {
            return indexes;
        }

        public void setIndexes(Map<String, IDX> indexes) {
            this.indexes = indexes;
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
}
