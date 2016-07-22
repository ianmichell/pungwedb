package com.pungwe.db.engine.io;

import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.engine.io.util.ByteBufferInputStream;
import com.pungwe.db.engine.io.util.ByteBufferOutputStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * <p>CommitLog is an append only file when logs all mutation events (insert, delete, update). This file is used
 * to rebuild a memory based collection (like the BTreeMap) for writes.</p>
 * <p>If the system fails for any reason (crash, power cycle, etc) then this log is replayed and the collection
 * rebuilt to the last written entry in the file.</p>
 * <p>This file is cycled when a regular basis whenever a memory collection is successfully written to disk.</p>
 * <p>
 * <p>Output format is as follows: [LENGTH][OP][TIMESTAMP][INTERVAL][VALUE]</p>
 * <ul>
 * <li><strong>LENGTH</strong>: Length of the entry</li>
 * <li><strong>OP</strong>: Single byte representing Insert 'I', Update 'U', Delete 'D'</li>
 * <li><strong>TIMESTAMP</strong>: 8 byte number representing milliseconds since epoch</li>
 * <li><strong>INTERVAL</strong>: 2 byte counter representing the interval between timestamps</li>
 * <li><strong>VALUE</strong>: </li>
 * </ul>
 */
public class CommitLog<V> implements Iterable<CommitLog.Entry<V>>, Closeable {

    public enum OP {
        INSERT((byte) 'I'), DELETE((byte) 'D'), UPDATE((byte) 'U');

        private byte type;

        OP(byte t) {
            type = t;
        }

        public byte getType() {
            return type;
        }

        public static OP fromType(byte t) {
            switch (t) {
                case 'I':
                    return INSERT;
                case 'D':
                    return DELETE;
                case 'U':
                    return UPDATE;
            }
            throw new IllegalArgumentException("Cannot determine type");
        }
    }

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Serializer<V> serializer;
    private final File file;
    private final RandomAccessFile randomAccessFile;
    private final AtomicLong writePosition = new AtomicLong();
    private final AtomicInteger interval = new AtomicInteger();
    private final AtomicLong lastWritten = new AtomicLong();

    public CommitLog(File file, Serializer<V> serializer) throws IOException {
        if (file.exists() && !file.isFile()) {
            throw new IOException("File: " + file.getName() + " is a directory!");
        } else if (!file.exists()) {
            file.createNewFile();
        }
        this.file = file;
        randomAccessFile = new RandomAccessFile(file, "rw");
        this.serializer = serializer;
    }

    public void append(OP operation, V value) throws IOException {
        lock.writeLock().lock();
        try {
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytesOut);
            long time = System.currentTimeMillis();
            out.write(operation.getType());
            out.writeLong(time);
            if (time != lastWritten.get()) {
                interval.set(0);
            }
            out.writeShort(interval.getAndIncrement());
            lastWritten.set(time);
            serializer.serialize(out, value);
            out.flush();
            byte[] bytes = bytesOut.toByteArray();
            ByteBuffer buffer = ByteBuffer.allocate(4 + bytes.length);
            ByteBufferOutputStream bufferOut = new ByteBufferOutputStream(buffer);
            bufferOut.writeInt(bytes.length);
            bufferOut.write(bytes);
            buffer.flip();
            // Thread safe.
            FileChannel channel = randomAccessFile.getChannel().position(writePosition.get());
            channel.write(buffer);
            writePosition.set(randomAccessFile.getChannel().position());
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        // Close the commit log
        randomAccessFile.close();
    }

    public void delete() throws IOException {
        this.close();
        this.file.delete();
    }

    @Override
    public Iterator<Entry<V>> iterator() {
        return new CommitLogIterator();
    }

    private class CommitLogIterator implements Iterator<Entry<V>> {

        private final AtomicLong position = new AtomicLong();
        private Entry<V> entry;

        public CommitLogIterator() {
            try {
                advance();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public boolean hasNext() {
            return entry != null;
        }

        @Override
        public Entry<V> next() {
            Entry<V> entry = this.entry;
            try {
                advance();
                return entry;
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        private void advance() throws IOException {
            if (position.get() < randomAccessFile.length()) {
                lock.readLock().lock();
                try {
                    FileChannel channel = randomAccessFile.getChannel().position(position.get());
                    // Allocate 4 bytes to byte buffer.
                    ByteBuffer length = ByteBuffer.allocate(4);
                    channel.read(length);
                    length.flip();
                    ByteBufferInputStream in = new ByteBufferInputStream(length);
                    int l = in.readInt();
                    // Create a new buffer of length and read in the data
                    ByteBuffer data = ByteBuffer.allocate(l);
                    channel.read(data);
                    data.flip();
                    // Set the read position
                    position.set(channel.position());
                    // Deserialize the entry
                    in = new ByteBufferInputStream(data);
                    byte type = in.readByte();
                    long timestamp = in.readLong();
                    short interval = in.readShort();
                    V value = serializer.deserialize(in);
                    this.entry = new CommitLog.Entry<>(OP.fromType(type), timestamp, interval, value);
                } finally {
                    lock.readLock().unlock();
                }
            } else {
                entry = null;
            }
        }
    }

    /**
     * Immutable entry of the commit log, used for reading.
     *
     * @param <V> the type of value
     */
    public static class Entry<V> {
        private final OP op;
        private final long timestamp;
        private final short interval;
        private final V value;

        public Entry(OP op, long timestamp, short interval, V value) {
            this.op = op;
            this.timestamp = timestamp;
            this.interval = interval;
            this.value = value;
        }

        public OP getOp() {
            return op;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public short getInterval() {
            return interval;
        }

        public V getValue() {
            return value;
        }
    }
}
