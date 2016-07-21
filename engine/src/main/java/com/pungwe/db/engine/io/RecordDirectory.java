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
package com.pungwe.db.engine.io;

import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.utils.UUIDGen;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ian on 18/07/2016.
 */
public class RecordDirectory<K, V> {

    static final Map<String, RecordDirectory> instances = new LinkedHashMap<>();

    private final File directory;
    private final String name;
    private Map<UUID, RecordFilePair<K, V>> recordIndexFiles = new TreeMap<>();
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    private RecordDirectory(String name, Serializer<K> keySerializer, Serializer<V> valueSerializer, File directory)
            throws IOException {
        this.directory = directory;
        this.name = name;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        loadFiles();
    }

    private void loadFiles() throws IOException {
        final Pattern p = Pattern.compile(name.replace("\\s", "_") + "_" + "(.*[^_])_(index|data)\\.db");
        String[] files = directory.list((dir, fileName) -> {
            Matcher m = p.matcher(fileName);
            return m.matches();
        });
        Map<UUID, String> keyFiles = new TreeMap<>(UUID::compareTo);
        Map<UUID, String> dataFiles = new TreeMap<>(UUID::compareTo);
        for (String file : files) {
            Matcher m = p.matcher(file);
            if (m.groupCount() < 3) {
                continue;
            }
            UUID id = UUID.fromString(m.group(1));
            String type = m.group(2);
            switch (type) {
                case "index": {
                    keyFiles.put(id, file);
                    break;
                }
                case "data": {
                    dataFiles.put(id, file);
                    break;
                }
            }
        }
        for (UUID key : keyFiles.keySet()) {
            if (!dataFiles.containsKey(key)) {
                continue;
            }
            recordIndexFiles.put(key, new RecordFilePair<>(
               key, new BasicRecordFile<>(new File(directory, keyFiles.get(key)), keySerializer),
                    new BasicRecordFile<>(new File(directory, keyFiles.get(key)), valueSerializer)
            ));
        }
    }

    public static <K, V> RecordDirectory getInstance(String name, String path, Serializer<K> keySerializer,
                                                     Serializer<V> valueSerializer) throws IOException {
        return getInstance(name, new File(path), keySerializer, valueSerializer);
    }

    public static <K, V> RecordDirectory getInstance(String name, File directory, Serializer<K> keySerializer,
                                                     Serializer<V> valueSerializer) throws IOException {
        if (directory.exists() && directory.isFile()) {
            throw new IOException("Specified directory is a file!");
        }
        // Create directory if not exists
        if (!directory.exists()) {
            directory.mkdirs();
        }
        // Return an instance of the record directory
        return instances.containsKey(name) ? instances.get(directory.getAbsolutePath()) :
                instances.put(name, new RecordDirectory<>(name, keySerializer, valueSerializer, directory));
    }

    @SuppressWarnings("unchecked")
    public RecordFile<K> getRecordKeyFile(UUID id) {
        if (recordIndexFiles.containsKey(id)) {
            return recordIndexFiles.get(id).keyFile;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public RecordFile<V> getRecordValueFile(UUID id) {
        if (recordIndexFiles.containsKey(id)) {
            return recordIndexFiles.get(id).valueFile;
        }
        return null;
    }

    public UUID[] getRecordFiles() {
        return recordIndexFiles.keySet().toArray(new UUID[0]);
    }

    /**
     * Creates a new record file pair (key and data) with a unique UUID (based on system time).
     *
     * @return the id of the new file pair.
     * @throws IOException
     */
    public UUID create() throws IOException {
        // UUID TYPE 1
        UUID id = UUIDGen.getTimeUUID();

        File keyFile = new File(directory, name.replaceAll("\\s", "_") + "_" + id.toString() + "_index.db");
        File valueFile = new File(directory, name.replaceAll("\\s", "_") + "_" + id.toString() + "_data.db");
        recordIndexFiles.put(id, new RecordFilePair<>(id, new BasicRecordFile<>(keyFile, keySerializer),
                new BasicRecordFile<>(valueFile, valueSerializer)));
        return id;
    }

    private static class RecordFilePair<K, V> {
        final UUID id;
        final RecordFile<K> keyFile;
        final RecordFile<V> valueFile;

        public RecordFilePair(UUID id, RecordFile<K> keyFile, RecordFile<V> valueFile) {
            this.id = id;
            this.keyFile = keyFile;
            this.valueFile = valueFile;
        }
    }
}
