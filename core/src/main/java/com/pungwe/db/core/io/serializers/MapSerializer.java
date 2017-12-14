package com.pungwe.db.core.io.serializers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by 917903 when 20/07/2016.
 */
public class MapSerializer<K,V> implements Serializer<Map<K,V>> {

    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public MapSerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public void serialize(DataOutput out, Map<K, V> value) throws IOException {
        // Write the map size
        out.writeInt(value.size());
        for (Map.Entry<K,V> entry : value.entrySet()) {
            keySerializer.serialize(out, entry.getKey());
            valueSerializer.serialize(out, entry.getValue());
        }
    }

    @Override
    public Map<K, V> deserialize(DataInput in) throws IOException {
        // Get map length
        int length = in.readInt();
        Map<K, V> map = new LinkedHashMap<>();
        for (int i = 0; i < length; i++) {
            K key = keySerializer.deserialize(in);
            V value = valueSerializer.deserialize(in);
            map.put(key, value);
        }
        return map;
    }

    @Override
    public String getKey() {
        return null;
    }
}
