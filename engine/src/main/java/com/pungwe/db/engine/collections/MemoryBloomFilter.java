package com.pungwe.db.engine.collections;

import com.pungwe.db.core.io.serializers.Serializer;

import java.util.BitSet;

/**
 * Created by 917903 on 12/07/2016.
 */
public class MemoryBloomFilter<E> extends BloomFilter<E> {

    private final BitSet filter = new BitSet();

    public MemoryBloomFilter(Serializer<E> elementSerializer, double falseProbabilityRate,
                             long expectedNumberOfElements) {
        super(elementSerializer);
    }

    @Override
    protected void put(long hash) {
        // Create a bit set
        BitSet set = BitSet.valueOf(new long[]{hash});

    }

    @Override
    protected boolean exists(long hash) {
        return false;
    }
}
