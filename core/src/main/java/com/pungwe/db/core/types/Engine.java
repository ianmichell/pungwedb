package com.pungwe.db.core.types;

import com.pungwe.db.core.concurrent.Promise;

import java.util.Map;

/**
 * Created by ian on 22/06/2016.
 */
public interface Engine {

    /**
     * Creates a new database instance with the default options
     *
     * @param name the name of the new database
     *
     * @return a promise object for the potentially newly created database
     */
    default Promise<Database> createDatabase(String name) {
        // FIXME: Get the config singleton and push the default database values
        return createDatabase(name, null);
    }

    /**
     * Creates a new database instance with the given options
     *
     * @param name the name of the new database
     *
     * @param config the configuration options for the new database
     *
     * @return a promise object for the potentially newly created database
     *
     */
    Promise<Database> createDatabase(String name, Map<String, Object> config);

    /**
     * Drops a database from storage
     *
     * @param name the name of the database to be dropped.
     */
    Promise<Void> dropDatabase(String name);

    /**
     * Fetches a database instance by name.
     *
     * @param name the name of the database to get
     *
     * @return a promise object containing an instance of the requested database when available
     */
    Promise<Database> getDatabase(String name);
}
