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
package com.pungwe.db.common.collections.btree;

import com.google.common.hash.BloomFilter;
import com.pungwe.db.common.io.MemoryStore;
import com.pungwe.db.common.io.RecordFile;
import com.pungwe.db.core.io.serializers.Serializer;

import java.util.*;

/**
 * Created by ian on 07/08/2016.
 */
public class MemoryBTreeMap<K, V> extends AbstractBTreeMap<K, V> {

    private final MemoryStore<Node<K, ?>> memoryStore;
    private final Serializer<K> bloomKeySerializer;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public MemoryBTreeMap(Comparator<K> keyComparator, Serializer<K> keySerializer, Serializer<K> bloomKeySerializer,
                          Serializer<V> valueSerializer, MemoryStore<Node<K, ?>> memoryStore, int maxNodeSize) {
        super(keyComparator, maxNodeSize);
        this.memoryStore = memoryStore;
        this.bloomKeySerializer = bloomKeySerializer;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public MemoryBTreeMap(Comparator<K> keyComparator, Serializer<K> keySerializer, Serializer<V> valueSerializer,
                          MemoryStore<Node<K, ?>> memoryStore, int maxNodeSize) {
        this(keyComparator, keySerializer, keySerializer, valueSerializer, memoryStore, maxNodeSize);
    }

    @Override
    protected BTreeEntry<K, V> putEntry(Entry<? extends K, ? extends V> entry) {
        return null;
    }

    @Override
    public BTreeEntry<K, V> getEntry(K key) {
        return null;
    }

    @Override
    protected V removeEntry(K key) {
        return null;
    }

    @Override
    public long sizeLong() {
        return 0;
    }

    @Override
    protected Iterator<Entry<K, V>> iterator(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return null;
    }

    @Override
    protected Iterator<Entry<K, V>> reverseIterator(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return null;
    }

    @Override
    public Iterator<Entry<K, V>> mergeIterator() {
        return null;
    }

    @Override
    protected Iterator<Entry<K, V>> mergeIterator(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return null;
    }

    @Override
    protected Node<K, ?> rootNode() {
        return null;
    }

    @Override
    protected BloomFilter<K> bloomFilter() {
        return null;
    }

    @Override
    public Set<BTreeEntry<K, V>> getEntries(Collection<K> keys) {
        return null;
    }

    @Override
    public void clear() {

    }
}
