package com.pungwe.db.core.io.store;

import com.pungwe.db.core.io.serializers.Serializer;

/**
 * Created by 917903 on 24/05/2016.
 */
public class AppendOnlyStore implements Store {

    @Override
    public Object get(long pointer, Serializer serializer) {
        return null;
    }

    @Override
    public long add(Object value, Serializer serializer) {
        return 0;
    }

    @Override
    public long update(long pointer, Object value, Serializer serializer) {
        return 0;
    }
}
