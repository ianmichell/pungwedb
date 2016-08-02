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

import com.pungwe.db.core.error.DatabaseRuntimeException;
import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.types.DBObject;
import com.pungwe.db.core.utils.UUIDGen;
import com.pungwe.db.core.utils.comparators.GenericComparator;
import com.pungwe.db.engine.collections.btree.AbstractBTreeMap;
import com.pungwe.db.engine.collections.btree.BTreeMap;
import com.pungwe.db.engine.collections.btree.ImmutableBTreeMap;
import com.pungwe.db.engine.io.BasicRecordFile;
import com.pungwe.db.engine.io.RecordFile;
import com.pungwe.db.engine.io.util.FileUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.pungwe.db.core.utils.Utils.createHash;
import static com.pungwe.db.core.utils.Utils.getValue;

/**
 * Created by ian on 28/07/2016.
 */
public class LSMTreeIndex {

    private final File directory;
    private final String name;
    private final Map<String, Boolean> fields;
    private final Comparator<Object> comparator;
    private final Serializer<Object> keySerializer = new WrappingObjectSerializer();
    private final Serializer<AbstractBTreeMap.Node<Object, ?>> nodeSerializer;
    private final ConcurrentNavigableMap<UUID, AbstractBTreeMap<Object, Object>> trees = new ConcurrentSkipListMap<>(
            UUID::compareTo);
    private boolean unique = false;
    private boolean primary = false;
    private final int maxMemoryTreeSize;

    public static LSMTreeIndex getInstance(File directory, String name, Map<String, Object> config) throws IOException {
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
        this.fields = ((Map<String, Boolean>)config.get("fields"));
        // Configure primary or unique
        primary = (Boolean)config.getOrDefault("primary", false);
        unique = primary ? true : (Boolean)config.getOrDefault("unique", false);
        // Max items in memory
        maxMemoryTreeSize = (Integer)config.getOrDefault("treeSize", 10000);
        // Construct the comparator
        this.comparator = buildComparator();
        // Construct the nodeSerializer
        this.nodeSerializer = ImmutableBTreeMap.serializer(comparator, keySerializer,
                new ObjectSerializer());
        configure(config);
    }

    public LSMEntry get(DBObject key) {
        LSMEntry value = null;

        // Contains the hashcode of the value and the
        Map<Long, List<AbstractBTreeMap.BTreeEntry<Object, Object>>> found = new LinkedHashMap<>();
        // Start at the top and work your way down..
        outer: for (Map.Entry<UUID, AbstractBTreeMap<Object, Object>> tree : trees.descendingMap().entrySet()) {
            // Build a collection of results by row key... they should naturally group by row key... Which we can then
            if (primary) {
                AbstractBTreeMap.BTreeEntry<Object, Object> entry = tree.getValue().getEntry(key.getId());
                if (entry == null) {
                    continue;
                }
                if (entry.isDeleted()) {
                    return null;
                }
                return new LSMEntry(entry.getKey(), entry.getValue());
            }
            // If it's not a primary index, it's a bit more complicated... Fields must contain all the fields in the key
            if (!fields.keySet().containsAll(key.keySet())) {
                return null;
            }
            // If there is a key there's a way
            for (Map.Entry<String, Object> keyEntry : key.entrySet()) {
                // If the value is a unique value, we can simply match on a single field
                if (unique && fields.size() == 1) {
                    AbstractBTreeMap.BTreeEntry<Object, Object> entry = tree.getValue().getEntry(keyEntry.getValue());
                    if (entry == null) {
                        continue outer;
                    }
                    // If the entry is not null, then
                    return new LSMEntry(entry.getKey(), entry.getValue());
                }
                /*
                 * If it's not unique, then we are going to have to collect the values by key... The comparator will
                 * handle the null sort key...
                 */
                AbstractBTreeMap.BTreeEntry<Object, Object> entry = tree.getValue().getEntry(new MultiFieldKey(
                        keyEntry.getKey(), keyEntry.getValue(), null, null));
                if (entry == null) {
                    continue outer;
                }

                if (unique && entry.isDeleted()) {
                    return null;
                } else if (unique) {
                    return new LSMEntry(entry.getKey(), entry.getValue());
                }

                // Add it to the found map
                Long hash = ((MultiFieldKey)entry.getKey()).getIdHash();
                if (!found.containsKey(hash)) {
                    found.put(hash, new ArrayList<>());
                }
                found.get(hash).add(entry);
            }
        }
        if (found.isEmpty()) {
            return null;
        }
        Optional<Map.Entry<Long, List<AbstractBTreeMap.BTreeEntry<Object, Object>>>> e = found.entrySet().stream()
                .filter(o -> o.getValue().size() == key.size()).findFirst();
        if (!e.isPresent()) {
            return null;
        }
        boolean deleted = e.get().getValue().stream().anyMatch(AbstractBTreeMap.BTreeEntry::isDeleted);
        if (deleted) {
            return null;
        }
        // Return the found value...
        return new LSMEntry(e.get().getValue().get(0).getKey(), e.get().getValue().get(0).getValue());
    }

    private long getHashCode(Object key) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            keySerializer.serialize(out, key);
            return createHash(bytes.toByteArray());
        } catch (Exception ex) {
            throw new DatabaseRuntimeException("Could not create a hash of a key: " + key);
        }

    }

    private boolean getSort(String key) {
        for (Map.Entry<String, Boolean> e : fields.entrySet()) {
            // the key passed into the method should at least start with
            if (key.startsWith(e.getKey())) {
                return e.getValue();
            }
        }
        return false;
    }

    /**
     * If the index is a primary index, then offset must be greater than 0. If it's not it can be anything...
     *
     * @param value the value being indexed
     * @param offset the offset (if a primary index) of the value
     */
    public boolean put(DBObject value, long offset) {
        // Primary indexes are never non-unique...
        if (!value.isValidId()) {
            throw new IllegalArgumentException("ID is not a valid key");
        }
        // Assign the insertion tree.
        AbstractBTreeMap<Object, Object> tree = trees.lastEntry().getValue();

        /*
         * All keys that are compound keys are stored in whole, unless they have arrays, in which case there
         * may be multiple copies of the same keys in the index, with the array values and row key providing uniqueness.
         * <p>
         * If a get is executed with a partial key, then a small scan of the index is executed to find the remaining
         * values... In short the most efficient mechanism to find an index entry is to use the whole key (or at least
         * the whole key with part of the array if any).
         */
        // Is this a primary index?
        if (primary) {
            // If the index is a primary index, then we cannot accept arrays on the _id key...
            if (Collection.class.isAssignableFrom(value.getId().getClass())) {
                // FIXME: Add better errors...
                throw new IllegalArgumentException("Arrays cannot be inserted into unique indexes");
            }
            // If the insert works, then it was successful.
            return putValue(value.getId(), offset) != null;
        }
        // Collect all the field values for the index...
        Map<String, Object> values = new LinkedHashMap<>();
        for (String field : fields.keySet()) {
            Optional<?> keyValue = getValue(field, value);
            // Insert the value into values or null...
            values.put(field, keyValue.orElse(null));
        }
        // It's not a primary index, but it's a single field key...
        if (fields.size() == 1) {
            // We only need one iteration here, so no need for a loop...
            Map.Entry<String, Object> e = values.entrySet().iterator().next();
            // Insert a single field value...
            return insertSingleField(tree, e.getKey(), e.getValue(), value.getId());
        }
        // Compound keys have an entry per field.
        UUID sort = UUIDGen.getTimeUUID();
        // Collection of compound keys
        List<MultiFieldKey> compoundKeys = new ArrayList<>(values.size());
        Long idHash = unique ? null : getHashCode(value.getId());
        for (Map.Entry<String, Object> e : values.entrySet()) {
            // Create a list of compound keys for insertion...
            compoundKeys.addAll(createCompoundKey(tree, e.getKey(), e.getValue(), idHash, sort));
        }
        // Cycle through each element and check if the key already exists... If so throw an exception...
        for (MultiFieldKey key : compoundKeys) {
            if (primary && hasKey(key)) {
                throw new IllegalArgumentException("Duplicate entry detected");
            }
        }
        for (MultiFieldKey key : compoundKeys) {
            if (putValue(key, value.getId()) == null) {
                return false;
            }
        }
        return true;
    }

    private Object putValue(Object key, Object value) {
        checkAndWriteTree();
        return trees.lastEntry().getValue().put(key, value);
    }

    @SuppressWarnings("unchecked")
    private List<MultiFieldKey> createCompoundKey(AbstractBTreeMap<Object, Object> tree, String field, Object value,
                                                  Long idHash, UUID sort) {
        List<MultiFieldKey> compoundKeys = new ArrayList<>();
        if (value == null) {
            compoundKeys.add(new MultiFieldKey(field, null, idHash, unique ? null : sort));
            return compoundKeys;
        }
        // Is it a collection?
        if (Collection.class.isAssignableFrom(value.getClass())) {
            // We only want unique values
            Set<Object> elements = ((Collection<Object>)value).stream().collect(Collectors.toSet());
            if (elements.size() == 0) {
                // If the set is empty, then rerun with this as a single null value...
                compoundKeys.add(new MultiFieldKey(field, null, idHash, unique  ? null : sort));
                return compoundKeys;
            }
            // Otherwise we can loop through all the elements and insert each one...
            compoundKeys.addAll(elements.stream().map(e -> new MultiFieldKey(field, e, idHash, unique ? null : sort))
                    .collect(Collectors.toList()));
            return compoundKeys;
        }
        compoundKeys.add(new MultiFieldKey(field, value, idHash, unique ? null : sort));
        return compoundKeys;
    }

    /**
     * Inserts a single field key into the index...
     * @param tree the tree to insert into
     * @param key field name
     * @param value the value of the key
     * @param id the id to link to
     * @return true if insertion succeeds...
     * @throws IllegalArgumentException if there is a unique constraint violation...
     */
    @SuppressWarnings("unchecked")
    private boolean insertSingleField(AbstractBTreeMap<Object, Object> tree, String key, Object value, Object id) {
        // Create or assign the row key, used if we have an array on one of the indexed keys and the index is not unique
        UUID sort = UUIDGen.getTimeUUID();
        if (value == null) {
            // If it has the key
            if (unique && hasKey(null)) {
                throw new IllegalArgumentException("duplicate key detected!");
            } else if (unique) {
                return putValue(null, id) != null;
            }
            // Otherwise insert a db object with a compound like key...
            return putValue(new MultiFieldKey(key, null, getHashCode(id), sort), id) != null;
        }
        // If the value is a collection, then insert an entry for each item in the collection...
        if (Collection.class.isAssignableFrom(value.getClass())) {
            // We only want unique entries...
            Set<Object> elements = ((Collection<Object>)value).stream().collect(Collectors.toSet());
            if (elements.size() == 0) {
                // If the set is empty, then rerun with this as a single null value...
                return insertSingleField(tree, key, null, id);
            }
            // Otherwise we can loop through all the elements and insert each one...
            for (Object e : elements) {
                if (unique && hasKey(e)) {
                    throw new IllegalArgumentException("duplicate key detected");
                } else if (unique) {
                    if (putValue(e, id) == null) {
                        throw new DatabaseRuntimeException("Could not insert array value into index");
                    }
                    continue;
                }
                // Insert into the tree...
                if (putValue(new MultiFieldKey(key, e, getHashCode(id), sort), id) == null) {
                    throw new DatabaseRuntimeException("Failed to insert: " + key);
                }
            }
            return true;
        }
        // Finally if we have an object, we can simply insert the appropriate key.
        if (unique) {
            if (hasKey(value)) {
                throw new IllegalArgumentException("Duplicate key detected");
            }
            return putValue(value, id) != null;
        }
        return putValue(new MultiFieldKey(key, value, getHashCode(id), sort), id) != null;
    }

    public boolean hasKey(Object key) {
        for (Map.Entry<UUID, AbstractBTreeMap<Object, Object>> e : trees.descendingMap().entrySet()) {
            if (e.getValue().containsKey(key) && !e.getValue().getEntry(key).isDeleted()) {
                return true;
            }
        }
        return false;
    }

    private void checkAndWriteTree() {
        // If the tree is of maxTreeSize then, write it to disk
        if (trees.lastEntry().getValue().size() == maxMemoryTreeSize) {
            // write the tree...
            File bloom = new File(directory, name + "_" + trees.lastEntry().getKey().toString() + "_bloom.db");
            File index = new File(directory, name + "_" + trees.lastEntry().getKey().toString() + "_index.db");
            // Record file
            try {
                RecordFile<AbstractBTreeMap.Node<Object, ?>> indexFile = new BasicRecordFile<>(index, nodeSerializer);
                ImmutableBTreeMap<Object, Object> tree = ImmutableBTreeMap.write(keySerializer, indexFile, bloom, name,
                        trees.lastEntry().getValue());
                trees.put(trees.lastEntry().getKey(), tree);
                trees.put(UUIDGen.getTimeUUID(), new BTreeMap<>(keySerializer, comparator, 1000));
            } catch (IOException ex) {
                throw new DatabaseRuntimeException("Could not write tree to disk");
            }
        }
    }

    /**
     * Updates the first closes match to the key. If the entire key is passed in then updating will not misfire, if a
     * partial key is passed in, then it will only update the first element (or all the elements if multi is true).
     *
     * @param originalKey the original key / search key for the record
     * @param value the updated record where the index keys will be picked.
     * @param multi multi update, if true updates all instances of key...
     */
    public void update(DBObject originalKey, DBObject value, boolean multi) {
        // Update a document based on it's original key
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

    private void configure(Map<String, Object> config) throws IOException {
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
                    files[i]), nodeSerializer);
            // Index File ID
            UUID id = UUID.fromString(fileId);
            // Open the index
            ImmutableBTreeMap<Object, Object> idx = ImmutableBTreeMap.getInstance(comparator,
                    keySerializer, indexFile, null, bloomFiles[i]);
            // Add the index to the tree...
            trees.put(id, idx);
        }
        // Create a writable Btree
        trees.put(UUIDGen.getTimeUUID(), new BTreeMap<>(keySerializer, comparator, 1000));
    }

    private Comparator<Object> buildComparator() {
        return (o1, o2) -> {
            if (o1 != null && MultiFieldKey.class.isAssignableFrom(o1.getClass())) {
                // Compare multi field keys
                boolean descending = getSort(((MultiFieldKey)o1).getField());
                Comparator<MultiFieldKey> c = new MultiFieldKeyComparator();
                if (descending) {
                    c = c.reversed();
                }
                return c.compare((MultiFieldKey)o1, (MultiFieldKey)o2);
            }
            // Otherwise compare!
            return GenericComparator.getInstance().compare(o1, o2);
        };
    }

    public String getName() {
        return name;
    }

    private static class MultiFieldKeyComparator implements Comparator<MultiFieldKey> {

        @Override
        public int compare(MultiFieldKey o1, MultiFieldKey o2) {
            int fieldCmp = o1.getField().compareTo(o2.getField());
            int cmp = GenericComparator.getInstance().compare(o1.getValue(), o2.getValue());
            if (fieldCmp == 0 && cmp != 0) {
                return cmp;
            }
            // If any of the sort keys are null, then it's the same..
            if (o2.getSort() == null || o2.getSort() == null) {
                return fieldCmp;
            }
            return GenericComparator.getInstance().compare(o1.getSort(), o2.getSort());
        }
    }

    public static class LSMEntry {
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

    // Scanning iterator... This will collate split index values...
    private static class LSMTreeIterator implements Iterator<LSMEntry> {

        // Store a reference to the outer class.
        private LSMTreeIndex index;
        // Collect the iterators...
        private NavigableMap<UUID, Iterator<Map.Entry<Object, Object>>> iterators =
                new ConcurrentSkipListMap<>(UUID::compareTo);
        // Use a navigable map to catalogue the entries in order of tree for the next value...
        private NavigableMap<UUID, AbstractBTreeMap.BTreeEntry<Object, Object>> found = new TreeMap<>(
                UUID::compareTo);
        // Current LSM Entry
        private Map.Entry<Object, Object> next;

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
            // Get the current key, then advance and return...
            LSMEntry current = new LSMEntry(next.getKey(), next.getValue());
            advance();
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
            Iterator<Map.Entry<UUID, Iterator<Map.Entry<Object, Object>>>> iteratorIT = iterators.descendingMap()
                    .entrySet().iterator();
            // Iterate...
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
            // Keys to remove from found...
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
            next = last.getValue();
        }
    }

    private static class MultiFieldKey {
        private final String field;
        private final Object value;
        private final UUID sort;
        private final Long idHash;

        public MultiFieldKey(String field, Object value, Long idHash, UUID sort) {
            this.field = field;
            this.value = value;
            this.sort = sort;
            this.idHash = idHash;
        }

        public String getField() {
            return field;
        }

        public Object getValue() {
            return value;
        }

        public UUID getSort() {
            return sort;
        }

        public Long getIdHash() {
            return idHash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MultiFieldKey that = (MultiFieldKey) o;

            if (field != null ? !field.equals(that.field) : that.field != null) return false;
            return value != null ? value.equals(that.value) : that.value == null;

        }

        @Override
        public int hashCode() {
            int result = field != null ? field.hashCode() : 0;
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }
    }

    private static class WrappingObjectSerializer implements Serializer<Object> {
        private Serializer<MultiFieldKey> multiFieldKeySerializer = new MultiFieldKeySerializer();
        private Serializer<Object> objectSerializer = new ObjectSerializer();

        @Override
        public void serialize(DataOutput out, Object value) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream d = new DataOutputStream(bytes);
            if (MultiFieldKey.class.isAssignableFrom(value.getClass())) {
                d.writeByte('M');
                multiFieldKeySerializer.serialize(d, (MultiFieldKey)value);
            } else {
                d.writeByte('O');
                objectSerializer.serialize(d, value);
            }
        }

        @Override
        public Object deserialize(DataInput in) throws IOException {
            byte t = in.readByte();
            switch (t) {
                case (byte)'M': {
                    return multiFieldKeySerializer.deserialize(in);
                }
                case (byte)'O': {
                    return objectSerializer.deserialize(in);
                }
            }
            return null;
        }

        @Override
        public String getKey() {
            return "WRAP:OBJ";
        }
    }

    private static class MultiFieldKeySerializer implements Serializer<MultiFieldKey> {

        private ObjectSerializer objectSerializer = new ObjectSerializer();

        @Override
        public void serialize(DataOutput out, MultiFieldKey value) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream d = new DataOutputStream(bytes);
            d.writeUTF(value.getField());
            objectSerializer.serialize(d, value.getValue());
            objectSerializer.serialize(d, value.getIdHash());
            objectSerializer.serialize(d, value.getSort());
            out.write(bytes.toByteArray());
        }

        @Override
        public MultiFieldKey deserialize(DataInput in) throws IOException {
            String key = in.readUTF();
            Object value = objectSerializer.deserialize(in);
            Long hash = (Long)objectSerializer.deserialize(in);
            UUID sort = (UUID)objectSerializer.deserialize(in);
            return new MultiFieldKey(key, value, hash, sort);
        }

        @Override
        public String getKey() {
            return "LSM:MFK";
        }
    }
}
