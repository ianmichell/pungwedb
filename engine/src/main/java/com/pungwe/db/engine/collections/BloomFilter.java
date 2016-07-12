/*
 * Copyright (C) 2016 Ian Michell.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pungwe.db.engine.collections;

import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.utils.Utils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;

/**
 * Created by ian on 12/07/2016.
 */
public abstract class BloomFilter<E> {

    private final Serializer<E> elementSerializer;
    private final long expectedElements;
    private final int hashes, bitSetSize;
    private final double bitsPerElement;
    private long elementsAdded = 0;
    private final BitSet bitset;

    /**
     * Constructs an empty Bloom filter. The total length of the Bloom filter will be
     * c*n.
     *
     * @param bitsPerElement is the number of bits used per element.
     * @param expectedElements is the expected number of elements the filter will contain.
     * @param hashes is the number of hash functions used.
     */
    public BloomFilter(Serializer<E> elementSerializer, double bitsPerElement, long expectedElements, int hashes) {
        this.elementSerializer = elementSerializer;
        this.expectedElements = expectedElements;
        this.hashes = hashes;
        this.bitsPerElement = bitsPerElement;
        this.bitSetSize = (int) Math.ceil(bitsPerElement * expectedElements);
        elementsAdded = 0;
        this.bitset = new BitSet(bitSetSize);
    }



    public void add(E element) {
        long hash = hash(element);
    }

    protected abstract void put(long hash);
    protected abstract boolean exists(long hash);

    public void addAll(Collection<E> elements) {
        elements.forEach(this::add);
    }

    public boolean contains(E element) {
        long hash = hash(element);
        return false;
    }

    private long hash(E element) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            elementSerializer.serialize(out, element);
            return Utils.createHash(bytes.toByteArray());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
