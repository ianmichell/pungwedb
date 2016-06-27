package com.pungwe.db.core.types;

import com.pungwe.db.core.concurrent.Promise;

import java.util.Map;
import java.util.Set;

/**
 * Created by ian on 18/06/2016.
 */
public interface Database {

    /**
     * @return the name of the database
     */
    String getName();

    /**
     * Gets a list of bucket names
     *
     * @return a promise with the list of buckets stored in this database on completion
     */
    Promise<Set<String>> getBucketNames();

    /**
     * Gets an instanceof of a bucket by name
     *
     * @param name the name of the bucket
     *
     * @return a promise with an instance of the requested bucket
     */
    Promise<Bucket<?>> getBucket(String name);

    /**
     * Creates a new bucket with the supplied settings
     *
     * @param name the name of the new bucket
     * @param options the options used to create teh bucket
     *
     * @return a promise for the instance of the newly created bucket
     */
    Promise<Bucket<?>> createBucket(String name, Map<String, Object> options);

    /**
     * Drops a database bucket
     *
     * @param name the name of the bucket to be dropped
     *
     * @return a promise with a void result when the database is dropped.
     */
    Promise<Void> dropBucket(String name);

}
