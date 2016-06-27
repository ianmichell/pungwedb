package com.pungwe.db.engine.types;

import com.pungwe.db.core.concurrent.Promise;
import com.pungwe.db.core.types.Database;
import com.pungwe.db.core.types.Engine;

import java.util.Map;

/**
 * Created by ian on 24/06/2016.
 */
public class DefaultEngine implements Engine {

    private static DefaultEngine INSTANCE;

    public static DefaultEngine getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new DefaultEngine();
        }
        return INSTANCE;
    }

    @Override
    public Promise<Database> createDatabase(String name, Map<String, Object> config) {
        return Promise.when(() -> {
            return null;
        });
    }

    @Override
    public Promise<Void> dropDatabase(String name) {
        return Promise.when(() -> {
            return null;
        });
    }

    @Override
    public Promise<Database> getDatabase(String name) {
        return Promise.when(() -> {
            return null;
        });
    }

}
