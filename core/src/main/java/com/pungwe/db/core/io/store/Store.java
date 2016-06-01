package com.pungwe.db.core.io.store;

import com.pungwe.db.core.io.serializers.Serializer;

/**
 * Created by 917903 on 24/05/2016.
 */
public interface Store {
    Object get(long pointer, Serializer serializer);
    long add(Object value, Serializer serializer);
    long update(long pointer, Object value, Serializer serializer);
}
