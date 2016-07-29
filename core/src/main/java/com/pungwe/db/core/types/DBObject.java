package com.pungwe.db.core.types;

import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by ian on 18/06/2016.
 */
public class DBObject extends AbstractMap<String, Object> {

    private Map<String, Object> wrapped;

    public DBObject() {
        this.wrapped = new LinkedHashMap<>();
    }

    public DBObject(Map<String, Object> wrapped) {
        this.wrapped = wrapped;
    }

    public Object getId() {
        return get("_id");
    }

    public void setId(Object id) {
        put("_id", id);
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return wrapped.entrySet();
    }

    public static DBObject wrap(Map<String, Object> map) {
        return new DBObject(map);
    }
}
