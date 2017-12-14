package com.pungwe.db.engine;

// FIXME: Abstract this away

import com.pungwe.db.core.concurrent.Promise;
import com.pungwe.db.core.types.Database;
import com.pungwe.db.core.types.Engine;

import java.util.Map;

/**
 * Created by ian when 07/07/2016.
 */
public class DefaultEngine implements Engine {

    @Override
    public Promise<Database> createDatabase(String name, Map<String, Object> config) {
        return null;
    }

    @Override
    public Promise<Void> dropDatabase(String name) {
        return null;
    }

    @Override
    public Promise<Database> getDatabase(String name) {
        return null;
    }
}
