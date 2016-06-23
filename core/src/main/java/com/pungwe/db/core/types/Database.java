package com.pungwe.db.core.types;

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
     * @return the list of buckets stored in this database
     */
    Set<String> getBucketNames();

    /**
     * Gets an instanceof of a bucket by name
     *
     * @param name the name of the bucket
     *
     * @return an instance of the bucket
     */
    Bucket getBucket(String name);

    /**
     * Creates a new bucket with the supplied settings
     *
     * @param name the name of the new bucket
     * @param options the options used to create teh bucket
     * @return the instance of the newly created bucket
     */
    Bucket createBucket(String name, Map<String, Object> options);

    /**
     * Drops a database bucket
     *
     * @param name the name of the bucket to be dropped
     */
    void dropBucket(String name);

}
