package com.pungwe.db.core.types;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by ian on 18/06/2016.
 */
public abstract class DBObject extends AbstractMap<String, Object> {

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return null;
    }

}
