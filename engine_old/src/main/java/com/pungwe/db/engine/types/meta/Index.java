package com.pungwe.db.engine.types.meta;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ian on 24/06/2016.
 */
public class Index {

    private final Map<String, Object> config;
    private final AtomicLong position = new AtomicLong();

    public Index(Map<String, Object> config, long position) {
        this.config = config;
        this.position.set(position);
    }

    public void setPosition(long position) {
        this.position.set(position);
    }

    public long getPosition() {
        return this.position.get();
    }

    public Map<String, Object> getConfig() {
        return ImmutableMap.copyOf(config);
    }
}
