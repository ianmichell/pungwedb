package com.pungwe.db.core.io.store;

import com.pungwe.db.core.io.serializers.Serializer;
import java.io.IOException;

/**
 * Created by 917903 on 24/05/2016.
 */
public interface Store {
    Object get(long pointer, Serializer serializer) throws IOException;
    long add(Object value, Serializer serializer) throws IOException;
    long update(long pointer, Object value, Serializer serializer) throws IOException;
}
