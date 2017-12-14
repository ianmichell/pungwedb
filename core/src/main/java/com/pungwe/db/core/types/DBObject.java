package com.pungwe.db.core.types;

import java.util.*;

/**
 * Created by ian on 18/06/2016.
 */
public class DBObject extends AbstractMap<String, Object> {

    private Map<String, Object> wrapped;

    public DBObject() {
        this.wrapped = new LinkedHashMap<>();
    }

    public DBObject(String key, Object value) {
        this();
        this.wrapped.put(key, value);
    }

    public DBObject(Map<String, Object> wrapped) {
        this.wrapped = wrapped;
    }

    // FIXME: This should not be part of the map...
    public Object getId() {
        return wrapped.get("_id");
    }

    public void setId(Object id) {
        wrapped.putIfAbsent("_id", id);
    }

    @Override
    public Object put(String key, Object value) {
        if (key.equals("_id")) {
            if (value == null) {
                throw new IllegalArgumentException("_id cannot be null");
            }
            return putIfAbsent(key, value);
        }
        return wrapped.put(key, value);
    }

    @Override
    public Object get(Object key) {
        return wrapped.get(key);
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return wrapped.entrySet();
    }

    public static DBObject wrap(Map<String, Object> map) {
        return new DBObject(map);
    }

    public boolean isValidId() {
        // Check that the id is value...
        return getId() != null && !Collection.class.isAssignableFrom(getId().getClass());
    }
}
