package com.pungwe.db.core.utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by ian when 21/06/2016.
 */
public final class ConfigSingleton extends AbstractMap<String, Object> {

    private static ConfigSingleton INSTANCE;
    private Map<String, Object> config;

    private ConfigSingleton() {
        config = ImmutableMap.of();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return ImmutableSet.copyOf(config.entrySet());
    }

    @SuppressWarnings("unchecked")
    public ConfigSingleton load(InputStream in) throws IOException {
        Yaml yaml = new Yaml();
        Map<String, Object> configMap = (Map<String, Object>)yaml.load(in);
        if (configMap == null) {
            return this;
        }
        this.config = ImmutableMap.copyOf(configMap);
        return this;
    }

    @Override
    public Object get(Object key) {
        return config.get(key);
    }

    @Override
    public boolean containsKey(Object key) {
        return config.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return config.containsValue(value);
    }

    public boolean containsPath(Object path) {
        String[] keys = ((String)path).split("\\.");
        Map<String, Object> config = this.config;
        for (int i = 0; i < keys.length - 1; i++) {
            Object v = config.get(keys[i]);
            if (v instanceof Map) {
                config = (Map<String, Object>) v;
            }
        }
        return config.containsKey(keys[keys.length - 1]);
    }

    public Object getByPath(String path) {
        if (!containsPath(path)) {
            return null;
        }
        String[] keys = ((String)path).split("\\.");
        Map<String, Object> config = this.config;
        for (int i = 0; i < keys.length - 1; i++) {
            Object v = config.get(keys[i]);
            if (v instanceof Map) {
                config = (Map<String, Object>) v;
            }
        }
        return config.get(keys[keys.length - 1]);
    }

    @Override
    public boolean isEmpty() {
        return config.isEmpty();
    }

    @Override
    public int size() {
        return config.size();
    }

    /**
     * Instanciates an instance of this singleton if it doesn't exist, or returns the existing instance.
     * @return
     */
    public static ConfigSingleton getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ConfigSingleton();
        }
        return INSTANCE;
    }
}
