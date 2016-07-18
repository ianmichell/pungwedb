package com.pungwe.db.engine.io;

import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.core.io.serializers.Serializer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by 917903 on 18/07/2016.
 */
public class SegmentedRecordFile<E> implements RecordFile<E> {

    private final String name;
    private final File parent;
    private final Serializer<E> serializer;
    private final int maxSegments;
    private final int writeBuffer;
    private final int segmentSize;
    private final AtomicInteger segment = new AtomicInteger();
    // Meta data holder.
    private Map<String, Object> meta;

    /**
     * <p>Creates an instance of a SegmentedRecordFile. This record file writes data in segments to the
     * parent directory. It also creates a meta data file, with segment information and other meta data.</p>
     *
     * @param name
     * @param parent
     * @param serializer
     * @throws IOException
     */
    public SegmentedRecordFile(String name, File parent, Serializer<E> serializer) throws IOException {
        // Default write buffer of 65K, unlimited segments
        this(name, parent, serializer, -1, 66560, Integer.MAX_VALUE);
    }

    public SegmentedRecordFile(String name, File parent, Serializer<E> serializer, int maxSegments) throws IOException {
        this(name, parent, serializer, -1, 66560, Integer.MAX_VALUE);
    }

    public SegmentedRecordFile(String name, File parent, Serializer<E> serializer, int maxSegments, int writeBuffer)
            throws IOException {
        this(name, parent, serializer, maxSegments, writeBuffer, Integer.MAX_VALUE);
    }

    public SegmentedRecordFile(String name, File parent, Serializer<E> serializer, int maxSegments, int writeBuffer,
                               int segmentSize) throws IOException {
        this.name = name;
        this.parent = parent;
        this.serializer = serializer;
        this.maxSegments = maxSegments;
        this.writeBuffer = writeBuffer;
        this.segmentSize = segmentSize;

        // Check if the parent file exists
        if (!parent.exists()) {
            parent.mkdirs();
        } else if (!parent.isDirectory()) {
            throw new IOException("A segmented record file requires a parent directory");
        }

        // Check for meta and create structure.
        createStructure();
    }

    private void createStructure() throws IOException {
        // Meta data
        File file = new File(parent, name + ".meta.bin");
        if (file.exists()) {
            // read meta
            readMeta();
            return;
        }
        if (!file.createNewFile()) {
            throw new IOException("Could not create meta file: " + name + "meta.bin");
        }
    }

    @SuppressWarnings("unchecked")
    private void readMeta() throws IOException {
        ObjectSerializer metaSerializer = new ObjectSerializer();
        RandomAccessFile raf = new RandomAccessFile(new File(parent, name + ".meta.bin"), "rw");
        try {
            meta = (Map<String, Object>) metaSerializer.deserialize(raf);
        } finally {
            raf.close();
        }
    }

    @Override
    public E get(long position) throws IOException {
        return null;
    }

    @Override
    public Reader<E> reader() throws IOException {
        return null;
    }

    @Override
    public Reader<E> reader(long position) throws IOException {
        return null;
    }

    @Override
    public Writer<E> writer() throws IOException {
        return null;
    }

    @Override
    public void setMetaData(Map<String, Object> metaData) throws IOException {
        if (meta == null) {
            meta = new LinkedHashMap<>();
        }
        this.meta.putAll(metaData);
    }

    @Override
    public Map<String, Object> getMetaData() throws IOException {
        if (meta == null) {
            readMeta();
        }
        return meta;
    }

    @Override
    public Iterator<E> iterator() {
        return null;
    }

    private class SegmentedReader<E> implements Reader<E> {

        private final AtomicLong positoion = new AtomicLong();


        @Override
        public long getPosition() throws IOException {
            return 0;
        }

        @Override
        public void setPosition(long position) throws IOException {

        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public E next() {
            return null;
        }
    }
}
