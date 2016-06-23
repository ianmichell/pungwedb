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
 * Created by ian on 21/06/2016.
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

    public ConfigSingleton load(InputStream in) throws IOException {
        Yaml yaml = new Yaml();
        this.config = ImmutableMap.copyOf((Map<String, Object>)yaml.load(in));
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
