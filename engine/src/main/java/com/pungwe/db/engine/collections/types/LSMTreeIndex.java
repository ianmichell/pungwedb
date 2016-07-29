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
package com.pungwe.db.engine.collections.types;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.pungwe.db.core.command.Query;
import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.types.DBObject;
import com.pungwe.db.core.utils.GenericComparator;
import com.pungwe.db.core.utils.UUIDGen;
import com.pungwe.db.engine.collections.btree.AbstractBTreeMap;
import com.pungwe.db.engine.collections.btree.BTreeMap;
import com.pungwe.db.engine.collections.btree.ImmutableBTreeMap;
import com.pungwe.db.engine.io.BasicRecordFile;
import com.pungwe.db.engine.io.RecordFile;
import com.pungwe.db.engine.io.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by ian on 28/07/2016.
 */
public class LSMTreeIndex {
    private final File directory;
    private final String name;
    private final Set<String> fields;
    private final Comparator<Object> comparator;
    private final Serializer<AbstractBTreeMap.Node<Object, ?>> serializer;
    private final ConcurrentNavigableMap<UUID, AbstractBTreeMap<Object, Object>> trees = new ConcurrentSkipListMap<>(
            UUID::compareTo);
    private boolean unique = false;

    public static LSMTreeIndex get(File directory, String name, Map<String, Object> config) throws IOException {
        return new LSMTreeIndex(directory, name, config);
    }

    @SuppressWarnings("unchecked")
    private LSMTreeIndex(File directory, String name, Map<String, Object> config) throws IOException {
        this.name = name;
        this.directory = directory;
        if (!config.containsKey("fields") && !Map.class.isAssignableFrom(config.get("fields").getClass())) {
            throw new IllegalArgumentException("Index config should be key value pairs - String key, Number value");
        }
        //
        this.fields = ((Map<String, Integer>)config.get("fields")).keySet();
        // Construct the comparator
        this.comparator = buildComparator((Map<String, Integer>)config.get("fields"));
        // Construct the serializer
        this.serializer = ImmutableBTreeMap.serializer(comparator, new ObjectSerializer(),
                new ObjectSerializer());
        configure(config);
    }

    public LSMEntry get(Object key) {
        // Start at the top and work your way down..
        for (Map.Entry<UUID, AbstractBTreeMap<Object, Object>> tree : trees.descendingMap().entrySet()) {
            AbstractBTreeMap.BTreeEntry<Object, Object> found = tree.getValue().getEntry(key);
            if (found != null && !found.isDeleted()) {
                return new LSMEntry(found.getKey(), found.getValue());
            }
        }
        return null;
    }

    /**
     * Creates a stream of the entire index. This is used for scanning the index for values and depending on size, can
     * might not be the fastest method of retrieval. If you have to scan an entire index and then
     *
     * @return a stream of entries...
     */
    public Stream<LSMEntry> stream() {
        return entrySet().stream();
    }

    public Set<LSMEntry> entrySet() {
        // Streams can be filtered.
        return new AbstractSet<LSMEntry>() {

            @Override
            public Iterator<LSMEntry> iterator() {
                return new LSMTreeIterator(LSMTreeIndex.this);
            }

            @Override
            public int size() {
                return -1;
            }
        };
    }

    public void add(DBObject object) {
        // If we have duplicates and they are allowed we store a copy of the id and the index key.
        if (fields.size() > 1 && unique) {

        }
    }

    public void update(Object originalKey, DBObject value) {
        // Update a document based on it's original key
    }

    private void configure(Map<String, Object> config) throws IOException {
        unique = (Boolean)config.getOrDefault("unique", false);
        // Find the files for bloom filter and index
        Pattern p = FileUtils.uuidFilePattern(name, "index.db", "_");
        Pattern pb = FileUtils.uuidFilePattern(name, "bloom.db", "_");
        // Filter the bloom files out of the directory
        File[] bloomFiles = directory.listFiles((dir, name) -> pb.matcher(name).matches());
        // Filter the index files out of the directory
        String[] files = directory.list((dir, name) -> p.matcher(name).matches());
        // ensure that there are the same number of files, or we will have a problem...
        if ((bloomFiles == null || files == null) || (bloomFiles.length != files.length)) {
            throw new IOException("Number of index files and bloom filters don't match. Run a repair");
        }
        // Load the  index files...
        for (int i = 0; i < files.length; i++) {
            String fileId = p.matcher(files[i]).group(1);
            // Open the index file
            RecordFile<AbstractBTreeMap.Node<Object, ?>> indexFile = new BasicRecordFile<>(new File(directory,
                    files[i]), serializer);
            // Index File ID
            UUID id = UUID.fromString(fileId);
            // Open the index
            ImmutableBTreeMap<Object, Object> idx = ImmutableBTreeMap.getInstance(comparator, new ObjectSerializer(),
                    indexFile, null, bloomFiles[i]);
            trees.put(id, idx);
        }
        // Create a writable Btree
        trees.put(UUIDGen.getTimeUUID(), new BTreeMap<>(new ObjectSerializer(), comparator, 1000));
    }

    private Comparator<Object> buildComparator(Map<String, Integer> fields) {
        if (fields.size() < 2 && unique) {
            return singleFieldComparator(fields);
        }
        // We can treat the values as maps... If the index unique value is set to false, then we store the id as well...
        return multiFieldComparator(fields);
    }

    @SuppressWarnings("unchecked")
    private Comparator<Object> multiFieldComparator(final Map<String, Integer> fields) {
        return (o1, o2) -> {
            DBObject db1 = DBObject.wrap((Map<String, Object>)o1);
            DBObject db2 = DBObject.wrap((Map<String, Object>)o2);
            int cmp = -1;
            for (Map.Entry<String, Integer> entry : fields.entrySet()) {
                if (db1.containsKey(entry.getKey()) && db2.containsKey(entry.getKey())) {
                    Comparator c = entry.getValue() < 1 ? GenericComparator.getInstance() : GenericComparator
                            .getInstance().reversed();
                    cmp = c.compare(db1.get(entry.getKey()), db2.get(entry.getKey()));
                    if (cmp != 0) {
                        return cmp;
                    }
                }
            }
            return cmp;
        };
    }

    private Comparator<Object> singleFieldComparator(Map<String, Integer> fields) {
        for (Map.Entry<String, Integer> entry : fields.entrySet()) {
            return entry.getValue() < 1 ? GenericComparator.getInstance() : GenericComparator.getInstance()
                    .reversed();
        }
        return GenericComparator.getInstance();
    }

    public String getName() {
        return name;
    }

    private static class LSMEntry {
        private final Object key;
        private final Object value;

        public LSMEntry(Object key, Object value) {
            this.key = key;
            this.value = value;
        }

        public Object getKey() {
            return key;
        }

        public Object getValue() {
            return value;
        }
    }

    private static class LSMTreeIterator implements Iterator<LSMEntry> {

        private LSMTreeIndex index;
        private NavigableMap<UUID, Iterator<Map.Entry<Object, Object>>> iterators =
                new ConcurrentSkipListMap<>(UUID::compareTo);
        // Use a navigable map to catalogue the entries in order of tree for the next value...
        private NavigableMap<UUID, AbstractBTreeMap.BTreeEntry<Object, Object>> found = new TreeMap<>(UUID::compareTo);

        private LSMEntry next;

        @SuppressWarnings("unchecked")
        private LSMTreeIterator(LSMTreeIndex index) {
            index.trees.entrySet().forEach(e -> {
                // We need to use a merge iterator as we want to include deletions...
                iterators.put(e.getKey(), e.getValue().mergeIterator());
            });
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public LSMEntry next() {
            // We always want to get the most recent version of the document...
            if (!hasNext()) {
                return null;
            }
            LSMEntry current = next;
            if (current != null) {
                advance();
            }
            return current;
        }

        private void advance() {
            // If next is null, then don't bother...
            if (next == null) {
                return;
            }
            // If there are no iterators, we're done..
            if (iterators.size() == 0) {
                next = null;
                return;
            }

            // Check that we have one for each item in the tree...
            Iterator<Map.Entry<UUID, Iterator<Map.Entry<Object, Object>>>> iteratorIT = iterators.entrySet().iterator();
            while (iteratorIT.hasNext()) {
                Map.Entry<UUID, Iterator<Map.Entry<Object, Object>>> e = iteratorIT.next();
                if (!e.getValue().hasNext()) {
                    iteratorIT.remove();
                    continue;
                }
                // Check if there is a key for it's id, if not then add it...
                if (!found.containsKey(e.getKey())) {
                    found.put(e.getKey(), (AbstractBTreeMap.BTreeEntry<Object, Object>)e.getValue().next());
                }
            }
            // Find the next value. If found size is less than 1, we're done...
            if (found.size() < 1) {
                next = null;
                return;
            }

            // Return the first value... We need to cycle through the list...
            Map.Entry<UUID, AbstractBTreeMap.BTreeEntry<Object, Object>> last = null;
            Set<Map.Entry<UUID, AbstractBTreeMap.BTreeEntry<Object, Object>>> remove = new LinkedHashSet<>();
            // Cycle through the entries and find the lowest key, whilst remembering the deletions...
            for (Map.Entry<UUID, AbstractBTreeMap.BTreeEntry<Object, Object>> e : found.entrySet()) {
                // We found e, so we need to remove it...
                if (e.getValue().isDeleted()) {
                    remove.add(e);
                }
                // If last is not set, then set last to foundIT.next()
                if (last == null) {
                    last = e;
                    continue;
                }
                // Compare last to current. If last is less than current, then we don't need to do anything
                int cmp = index.comparator.compare(last.getValue().getKey(), e.getValue().getKey());
                // If they're the same, then set last
                if (cmp == 0) {
                    // Add last to remove...
                    remove.add(last);
                    // Ensure the current object is not deleted
                    if (e.getValue().isDeleted()) {
                        // Add the entry to the removals collection...
                        remove.add(e);
                        // Reset last...
                        last = null;
                        continue;
                    }
                    last = e;
                    continue;
                }

                // If last is higher than the current entry, then set last to e and continue
                if (cmp > 0) {
                    last = e;
                }
            }
            // Remove stale entries...
            remove.forEach(e -> found.remove(e.getKey()));
            // Check that last is not null or deleted...
            if (last == null || last.getValue().isDeleted()) {
                advance();
                return;
            }
            // Remove last from found
            found.remove(last.getKey());
            // set next...
            next = new LSMEntry(last.getValue().getKey(), last.getValue().getValue());
        }
    }
}
