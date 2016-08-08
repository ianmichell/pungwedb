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

import com.pungwe.db.common.collections.btree.AbstractBTreeMap;
import com.pungwe.db.common.collections.btree.BTreeMap;
import com.pungwe.db.core.error.DatabaseRuntimeException;
import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.types.Bucket;
import com.pungwe.db.core.types.DBObject;
import com.pungwe.db.core.utils.ConfigSingleton;
import com.pungwe.db.core.utils.UUIDGen;
import com.pungwe.db.core.utils.comparators.GenericComparator;
import com.pungwe.db.engine.collections.btree.ImmutableBTreeMap;
import com.pungwe.db.engine.io.BasicRecordFile;
import com.pungwe.db.common.io.RecordFile;
import com.pungwe.db.engine.io.util.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Matcher;
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
    private final Bucket.IndexConfig config;
    private final Comparator<Object> comparator;
    private final Serializer<Object> bloomSerializer;
    private final Serializer<AbstractBTreeMap.Node<Object, ?>> nodeSerializer;
    private final ConcurrentNavigableMap<UUID, AbstractBTreeMap<Object, Object>> trees = new ConcurrentSkipListMap<>(
            UUID::compareTo);

    private boolean closed = false;

    public static LSMTreeIndex getInstance(File directory, Bucket.IndexConfig config) throws IOException {
        return new LSMTreeIndex(directory, config);
    }


    public static LSMTreeIndex build(File directory, Bucket.IndexConfig config, Stream<DBObject> valueStream)
            throws IOException {
        // Create a temporary directory. We want to merge after we've built, so that our trees are at their maximum size
        String tmpPath = (String)ConfigSingleton.getInstance().getByPath("engine.tmp.path");
        File tmp = null;
        if (StringUtils.isNotBlank(tmpPath)) {
            tmp = Files.createTempDirectory(Paths.get(tmpPath), config.getName()).toFile();
        } else {
            tmp = Files.createTempDirectory(config.getName()).toFile();
        }
        /*
         * Create the index within the temporary directory (this is useful for rebuilding, because if it fails, we need
         * only delete the folder it was in!
         */
        LSMTreeIndex tmpIndex = LSMTreeIndex.getInstance(tmp, config);
        /*
         * This could take a long time depending on how many records are contained in the stream... On a partitioned
         * system, we would only be indexing the data contained on this node..
         */
        valueStream.forEach(tmpIndex::put);
        // Once the values have been indexed, we need to perform a merge...
        tmpIndex.merge();
        // Close the index
        tmpIndex.close();
        /*
         * Move the files to the parent directory and return a new copy of the index... We should only have the relevant
         * in this directory...
         */
        File[] indexFiles = tmp.listFiles();
        if (indexFiles == null || indexFiles.length < 1) {
            throw new IOException("Could not build new index...");
        }
        for (File f : indexFiles) {
            Files.move(Paths.get(f.toURI()), Paths.get(new File(directory, f.getName()).toURI()),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        }
        // Return an instance of the LSMTreeIndex....
        return LSMTreeIndex.getInstance(directory, config);
    }

    /**
     * Creates and builds a new index, that replaces an old index...
     *
     * @param oldIndex the index being rebuilt
     * @param valueStream the values being indexed
     * @return a new index to replace the old index
     *
     * @throws IOException if there is a problem creating the new index or destroying the old...
     */
    public static LSMTreeIndex rebuild(LSMTreeIndex oldIndex, Stream<DBObject> valueStream) throws IOException {
        // Creates a new index to replace oldIndex...
        LSMTreeIndex newIndex = build(oldIndex.directory, oldIndex.config, valueStream);
        // If the new index was created successfully... Delete the old one...
        oldIndex.drop();
        // Return the newer index...
        return newIndex;
    }

    @SuppressWarnings("unchecked")
    private LSMTreeIndex(File directory, Bucket.IndexConfig config) throws IOException {
        this.directory = directory;
        // Construct the comparator
        this.comparator = buildComparator();
        this.bloomSerializer = new WrappingObjectSerializer(!config.isUnique());
        Serializer<Object> keySerializer = new WrappingObjectSerializer();
        // Construct the nodeSerializer
        this.nodeSerializer = ImmutableBTreeMap.serializer(comparator, keySerializer,
                new ObjectSerializer());
        this.config = config;
        configure();
    }

    public LSMEntry get(DBObject key) {

        if (closed) {
            throw new IllegalArgumentException("This index has been closed");
        }
        LSMEntry value = null;

        // Contains the hashcode of the value and the
        Map<Long, List<AbstractBTreeMap.BTreeEntry<Object, Object>>> found = new LinkedHashMap<>();
        // Work up from the bottom...
        outer: for (Map.Entry<UUID, AbstractBTreeMap<Object, Object>> tree : trees.entrySet()) {
            // Build a collection of results by row key... they should naturally group by row key... Which we can then
            if (config.isPrimary()) {
                AbstractBTreeMap.BTreeEntry<Object, Object> entry = tree.getValue().getEntry(key.getId());
                if (entry == null) {
                    continue;
                }
                if (entry.isDeleted()) {
                    return null;
                }
                return newerInstance(tree.getKey(), entry);
            }
            // If it's not a primary index, it's a bit more complicated... Fields must contain all the fields in the key
            if (!config.hasFields(key.keySet())) {
                return null;
            }
            // If there is a key there's a way
            for (Map.Entry<String, Object> keyEntry : key.entrySet()) {
                // If the value is a unique value, we can simply match on a single field
                if (config.isUnique() && config.getFields().size() == 1) {
                    AbstractBTreeMap.BTreeEntry<Object, Object> entry = tree.getValue().getEntry(keyEntry.getValue());
                    if (entry == null) {
                        continue outer;
                    }
                    return newerInstance(tree.getKey(), entry);
                }
                /*
                 * If it's not unique, then we are going to have to collect the values by key... The comparator will
                 * handle the null sort key...
                 */
//                AbstractBTreeMap.BTreeEntry<Object, Object> entry = tree.getValue().getEntry(new MultiFieldKey(
//                        keyEntry.getKey(), keyEntry.getValue(), null, null));
                AbstractBTreeMap.BTreeEntry<Object, Object> entry = (AbstractBTreeMap.BTreeEntry<Object, Object>)tree
                        .getValue().tailMap(new MultiFieldKey(keyEntry.getKey(), keyEntry.getValue(), null, null))
                        .firstEntry();
                if (entry == null) {
                    continue outer;
                }

                if (config.isUnique() && entry.isDeleted()) {
                    return null;
                } else if (config.isUnique()) {
                    return newerInstance(tree.getKey(), entry);
                }

                // Add it to the found map
                Long hash = ((MultiFieldKey)entry.getKey()).getIdHash();
                if (!found.containsKey(hash)) {
                    found.put(hash, new ArrayList<>());
                }
                List<AbstractBTreeMap.BTreeEntry<Object, Object>> foundKeys = found.get(hash);
                foundKeys.add(newerEntry(tree.getKey(), entry));
                if (foundKeys.size() == key.size()) {
                    break outer;
                }
            }
        }
        if (found.isEmpty()) {
            return null;
        }
        // Errrrrrrrr.....
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

    private LSMEntry newerInstance(UUID treeKey, AbstractBTreeMap.BTreeEntry<Object, Object> entry) {
        // If it's null return null...
        if (entry == null) {
            return null;
        }
        // Check for a newer item, if there is, then return that!
        LSMEntry newerEntry = new LSMEntry(entry.getKey(), entry.getValue());
        // Check that we're not on the last tree! If we are, then return the current entry as there is no newer!
        if (trees.lastKey().equals(treeKey)) {
            return newerEntry;
        }

        // Get the newer entry (if any)... A false positive gets a search...
        AbstractBTreeMap.BTreeEntry<Object, Object> newer = newerEntry(treeKey, entry);
        // If newer is not null and deleted, then it's deleted damnit! Return null...
        if (newer != null && newer.isDeleted()) {
            return null;
        } else if (newer != null) {
            // If it's not deleted we return the newer key!
            return new LSMEntry(newer.getKey(), newer.getValue());
        }
        // Otherwise we return what we have... :(
        return newerEntry;
    }

    private AbstractBTreeMap.BTreeEntry<Object, Object> newerEntry(UUID treeKey, AbstractBTreeMap.
            BTreeEntry<Object, Object> entry) {
        // Current used to store the last most recent entry...
        AbstractBTreeMap.BTreeEntry<Object, Object> current = entry;
        /*
         * Get a head map of the trees in reverse order, so we're looking at the latest btree and working backwards to
         * the id of the current tree (which we exclude).
         */
        for (Map.Entry<UUID, AbstractBTreeMap<Object, Object>> tree : trees.descendingMap().headMap(treeKey, false)
                .entrySet()) {
            // Get the newer entry (if any)... A false positive gets a search...
            AbstractBTreeMap.BTreeEntry<Object, Object> newer = tree.getValue().getEntry(entry.getKey());
            //
            if (newer != null) {
                current = newer;
            }
        }
        return current;
    }

    private long getHashCode(Object key) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            new ObjectSerializer().serialize(out, key);
            return createHash(bytes.toByteArray());
        } catch (Exception ex) {
            throw new DatabaseRuntimeException("Could not create a hash of a key: " + key);
        }

    }

    private boolean getSort(String key) {
        for (Bucket.IndexField field : config.getFields()) {
            // the key passed into the method should at least start with
            if (key.startsWith(field.getName())) {
                return field.isDescending();
            }
        }
        return false;
    }

    public boolean put(DBObject value) {
        return put(value, -1);
    }
    /**
     * If the index is a primary index, then offset must be greater than 0. If it's not it can be anything...
     *
     * @param value the value being indexed
     * @param offset the offset (if a primary index) of the value
     */
    public boolean put(DBObject value, long offset) {
        if (closed) {
            throw new IllegalArgumentException("This index has been closed");
        }
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
        if (config.isPrimary()) {
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
        for (Bucket.IndexField field : config.getFields()) {
            Optional<?> keyValue = getValue(field.getName(), value);
            // Insert the value into values or null...
            values.put(field.getName(), keyValue.orElse(null));
        }
        // It's not a primary index, but it's a single field key...
        if (config.getFields().size() == 1) {
            // We only need one iteration here, so no need for a loop...
            Map.Entry<String, Object> e = values.entrySet().iterator().next();
            // Insert a single field value...
            return insertSingleField(tree, e.getKey(), e.getValue(), value.getId());
        }
        // Compound keys have an entry per field.
        UUID sort = UUIDGen.getTimeUUID();
        // Collection of compound keys
        List<MultiFieldKey> compoundKeys = new ArrayList<>(values.size());
        Long idHash = config.isUnique() ? null : getHashCode(value.getId());
        for (Map.Entry<String, Object> e : values.entrySet()) {
            // Create a list of compound keys for insertion...
            compoundKeys.addAll(createCompoundKey(tree, e.getKey(), e.getValue(), idHash, sort));
        }
        // Cycle through each element and check if the key already exists... If so throw an exception...
        for (MultiFieldKey key : compoundKeys) {
            if (config.isPrimary() && hasKey(key)) {
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
            compoundKeys.add(new MultiFieldKey(field, null, idHash, config.isUnique() ? null : sort));
            return compoundKeys;
        }
        // Is it a collection?
        if (Collection.class.isAssignableFrom(value.getClass())) {
            // We only want unique values
            Set<Object> elements = ((Collection<Object>)value).stream().collect(Collectors.toSet());
            if (elements.size() == 0) {
                // If the set is empty, then rerun with this as a single null value...
                compoundKeys.add(new MultiFieldKey(field, null, idHash, config.isUnique()  ? null : sort));
                return compoundKeys;
            }
            // Otherwise we can loop through all the elements and insert each one...
            compoundKeys.addAll(elements.stream().map(e -> new MultiFieldKey(field, e, idHash,
                    config.isUnique() ? null : sort)).collect(Collectors.toList()));
            return compoundKeys;
        }
        compoundKeys.add(new MultiFieldKey(field, value, idHash, config.isUnique() ? null : sort));
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
            if (config.isUnique() && hasKey(null)) {
                throw new IllegalArgumentException("duplicate key detected!");
            } else if (config.isUnique()) {
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
                if (config.isUnique() && hasKey(e)) {
                    throw new IllegalArgumentException("duplicate key detected");
                } else if (config.isUnique()) {
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
        if (config.isUnique()) {
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
        if (trees.lastEntry().getValue().size() == config.getMemoryTreeSize()) {
            writeTreeToDisk();
            // Create a new tree...
            trees.put(UUIDGen.getTimeUUID(), new BTreeMap<>(bloomSerializer, comparator, 1000));
        }
    }

    private void writeTreeToDisk() {
        // write the tree...
        File bloom = new File(directory, config.getName() + "_" + trees.lastEntry().getKey().toString()
                + "_bloom.db");
        File index = new File(directory, config.getName() + "_" + trees.lastEntry().getKey().toString()
                + "_index.db");
        // Record file
        try {
            RecordFile<AbstractBTreeMap.Node<Object, ?>> indexFile = new BasicRecordFile<>(index, nodeSerializer);
            ImmutableBTreeMap<Object, Object> tree = ImmutableBTreeMap.write(bloomSerializer, indexFile, bloom,
                    config.getName(), trees.lastEntry().getValue());
            trees.put(trees.lastEntry().getKey(), tree);
            // force commit
            indexFile.writer().commit();
        } catch (IOException ex) {
            throw new DatabaseRuntimeException("Could not write tree to disk");
        }
    }

    /**
     * Updates the entry with the value passed in. The value must contain the fields being updated as well
     * as the object _id key. The _id key is hashed and used to ensure that the value being updated is correct.
     *
     * If multi is set to true, then the hash of the key is ignored and every match will be updated...
     *
     * @param value the updated record where the index keys will be picked.
     * @param multi multi update, if true updates all instances of key...
     */
    public void update(DBObject value, boolean multi) {
        // Update a document based on it's original key
        if (closed) {
            throw new IllegalArgumentException("This index has been closed");
        }
    }

    /**
     * Merges the btrees that are contained within the LSM Tree... Removes stale data...
     *
     * @throws IOException
     */
    public void merge() throws IOException {
        // FIXME: Make me do something...
        if (closed) {
            throw new IllegalArgumentException("This index has been closed");
        }
    }

    public void gc() throws IOException {
        if (closed) {
            throw new IllegalArgumentException("This index has been closed");
        }
    }

    /**
     * This returns a stream of the entire index. It's non-collating, so any multi key values will be returned
     * as single value entries... This means that if you have a compound key of {key: false, another: false}, results
     * will be returned in key order from each tree in the LSMTree ( {key: {key=value}, value: object},
     * {key: {another=value}, value: object }, etc).
     *
     * @return a stream of entries...
     */
    public Stream<LSMEntry> stream() {
        return entrySet().stream();
    }

    /**
     * Returns a deletion stream for garbage collection. This is used by the bucket to remove stale records...
     *
     * @return a stream of deleted entries...
     */
    public Stream<LSMEntry> deletions() {
        if (closed) {
            throw new IllegalArgumentException("This index has been closed");
        }
        return null;
    }

    public Set<LSMEntry> entrySet() {
        if (closed) {
            throw new IllegalArgumentException("This index has been closed");
        }
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

    private void configure() throws IOException {
        // Find the files for bloom filter and index
        Pattern p = FileUtils.uuidFilePattern(config.getName(), "index.db", "_");
        Pattern pb = FileUtils.uuidFilePattern(config.getName(), "bloom.db", "_");
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
            Matcher m = p.matcher(files[i]);
            if (!m.find()) {
                throw new IOException("Could not get the file id");
            }
            String fileId = m.group(1);
            // Open the index file
            RecordFile<AbstractBTreeMap.Node<Object, ?>> indexFile = new BasicRecordFile<>(new File(directory,
                    files[i]), nodeSerializer);
            // Index File ID
            UUID id = UUID.fromString(fileId);
            // Open the index
            ImmutableBTreeMap<Object, Object> idx = ImmutableBTreeMap.getInstance(comparator,
                    bloomSerializer, indexFile, null, bloomFiles[i]);
            // The index was closed (we should also look at a log replay)
            if (idx.size() < config.getMemoryTreeSize()) {
                trees.put(id, BTreeMap.from(bloomSerializer, idx, 1000));
            }
            // Add the index to the tree...
            trees.put(id, idx);
        }
        // Create a writable Btree
        if (trees.size() > 0 && BTreeMap.class.isAssignableFrom(trees.lastEntry().getValue().getClass())) {
            return;
        }
        trees.put(UUIDGen.getTimeUUID(), new BTreeMap<>(bloomSerializer, comparator, 1000));
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

    private void drop() throws IOException {
        if (closed) {
            throw new IllegalArgumentException("This index has been closed");
        }
        for (UUID key : trees.keySet()) {
            File indexFile = new File(directory, config.getName() + "_" + key.toString() + "_index.db");
            File bloomFile = new File(directory, config.getName() + "_" + key.toString() + "_bloom.db");
            // Delete the index file
            if (!indexFile.delete()) {
                throw new IOException("Could not delete index file: " + indexFile.getName());
            }
            // Delete the bloom file..
            if (!bloomFile.delete()) {
                throw new IOException("Could not delete bloom file: " + bloomFile.getName());
            }
        }
        trees.clear();
    }

    private void close() {
        writeTreeToDisk();
        closed = true;
    }

    public String getName() {
        return config.getName();
    }

    public Bucket.IndexConfig getConfig() {
        return config;
    }

    private static class MultiFieldKeyComparator implements Comparator<MultiFieldKey> {

        @Override
        public int compare(MultiFieldKey o1, MultiFieldKey o2) {
            // Get the hash comparison...
            int hashCmp = 0;
            // These objects are obviously very different...
            if (o1.getIdHash() != null && o2.getIdHash() != null) {
                hashCmp = Long.compare(o1.getIdHash(), o2.getIdHash());
            } else if (o1.getIdHash() == null && o2.getIdHash() != null) {
                hashCmp = -1;
            } else if (o2.getIdHash() == null) {
                hashCmp = 1;
            }
            // If the hashCmp is 0... Then we're dealing with the same object!
            int fieldCmp = o1.getField().compareTo(o2.getField());
            // If the field comparison is 0 and the hash comparison is 0, then return 0, not point going further...
            if (fieldCmp == 0 && hashCmp == 0) {
                return 0;
            }
            // If the field comparison is different... Then return that...
            if (fieldCmp != 0) {
                return fieldCmp;
            }
            // If however, the field comparison is the same, then we can check the value...
            int valueCmp = GenericComparator.getInstance().compare(o1.getValue(), o2.getValue());
            // If the value comparison is not 0, then return that as it's clearly different...
            if (valueCmp != 0) {
                return valueCmp;
            }
            /*
             * If field comparison is the same and the value comparison is the same and we have a different hash, then
             * the last step is to check the sort as this should order things in the correct place...
             */
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

    /**
     * This will iterate through the entire LSMTree index. It's non-collating, so any multi key values will be returned
     * as single value entries... This means that if you have a compound key of {key: false, another: false}, results
     * will be returned in key order from each tree in the LSMTree ( {key: {key=value}, value: object},
     * {key: {another=value}, value: object }, etc).
     */
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
        private final Serializer<MultiFieldKey> multiFieldKeySerializer;
        private final Serializer<Object> objectSerializer;

        public WrappingObjectSerializer(boolean bloom) {
            // If the bloom is set to true, then it's a non-unique index and life could get a bit trickier.
            multiFieldKeySerializer = new MultiFieldKeySerializer(bloom);
            objectSerializer = new ObjectSerializer();
        }

        public WrappingObjectSerializer() {
            this(false);
        }

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
            out.write(bytes.toByteArray());
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
        private final boolean bloom;

        public MultiFieldKeySerializer(boolean bloom) {
            this.bloom = bloom;
        }

        @Override
        public void serialize(DataOutput out, MultiFieldKey value) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream d = new DataOutputStream(bytes);
            d.writeUTF(value.getField());
            objectSerializer.serialize(d, value.getValue());
            if (!bloom) {
                objectSerializer.serialize(d, value.getIdHash());
                objectSerializer.serialize(d, value.getSort());
            }
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
