package com.pungwe.db.core.registry;

import com.pungwe.db.core.io.serializers.EncryptedSerializer;
import com.pungwe.db.core.io.serializers.LZ4Serializer;
import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.core.io.serializers.Serializer;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by ian on 20/06/2016.
 */
@Deprecated
public class SerializerRegistry {

    private static SerializerRegistry INSTANCE;

    final Map<String, Serializer> serializers;

    /* Default Serializers */
    static {
        getIntance().register(new ObjectSerializer());
        /* Compressed Object Serializer */
        getIntance().register(new LZ4Serializer(new ObjectSerializer()));
        /* Encrypted Object Serializer */
        getIntance().register(new EncryptedSerializer(new ObjectSerializer()));
        /* Encrypted and Compressed Object Serializer */
//        getIntance().register(new EncryptedSerializer(new LZ4Serializer(new ObjectSerializer())));
    }

    private SerializerRegistry() {
        serializers = new LinkedHashMap<>();
    }

    /**
     * Returns an instance of this factory
     *
     * @return the registry instance
     */
    public static SerializerRegistry getIntance() {
        if (INSTANCE == null) {
            INSTANCE = new SerializerRegistry();
        }
        return INSTANCE;
    }

    /**
     * Registers a serializer in the serializer registry
     *
     * @param serializers the serializers (1 to n)
     */
    public void register(Serializer... serializers) {
        for (Serializer serializer : serializers) {
            this.serializers.put(serializer.getKey(), serializer);
        }
    }

    /**
     * Retrieves a serializer by key
     *
     * @param serializer the name / key of the serializer
     *
     * @return the serializer if registered or null
     */
    public Serializer getByKey(String serializer) {
        return serializers.get(serializer);
    }

    public boolean hasSerializer(String key) {
        return serializers.containsKey(key);
    }
}
