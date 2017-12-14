package com.pungwe.db.engine.io;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.engine.utils.Constants;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

// FIXME: This might never be used, but proves the concept!
/**
 * Created by 917903 on 06/07/2016.
 */
public class CachedRecordFile<E> implements RecordFile<E> {

    private final Cache<Long, Record> cache;
    private final File file;
    private final Serializer<E> serializer;
    private final CachedRecordWriter writer;

    public CachedRecordFile(final File file, final Serializer<E> serializer, final int cacheSize) throws IOException {
        this.file = file;
        this.serializer = serializer;
        this.cache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
        writer = new CachedRecordWriter();
    }

    @Override
    public E get(final long position) {
        try {
            Record record = cache.get(position, () -> {
                FileChannel channel = new RandomAccessFile(file, "r").getChannel();
                try {
                    channel.position(position);
                    return read(channel).get();
                } finally {
                    channel.close();
                }
            });
            return record.getValue();
        } catch (ExecutionException ex) {
            return null;
        }
    }

    @Override
    public Iterator<E> iterator() {
        try {
            return reader();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Reader<E> reader() throws IOException {
        return new CachedRecordReader();
    }

    @Override
    public Reader<E> reader(long position) throws IOException {
        return new CachedRecordReader(position);
    }

    public long append(E value) throws IOException {
        return writer.append(value);
    }

    public Writer<E> writer() throws IOException {
        return writer;
    }

    private Optional<Record> read(FileChannel channel) throws IOException {
        if (channel.position() >= channel.size()) {
            return Optional.empty();
        }
        ByteBuffer buffer = ByteBuffer.allocate(5).order(ByteOrder.BIG_ENDIAN);
        channel.read(buffer);
        buffer.flip();
        ByteBufferInputStream in = new ByteBufferInputStream(buffer);
        // Ensure what we have is a record
        byte t = in.readByte();
        if (t != 'R') {
            throw new IOException("Data at position:" + channel.position() + " is not a record: " + t);
        }
        // Get the record length
        int length = in.readInt();
        // Once we have the length, create a new byte buffer
        buffer = ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN);
        // Read into the buffer
        channel.read(buffer);
        buffer.flip();
        // Replace the input stream
        in = new ByteBufferInputStream(buffer);
        Optional<Record> v = Optional.empty();
        // deserialize the record into an object.
        E e = serializer.deserialize(in);
        if (e != null) {
            v = Optional.of(new Record(e, length, channel.position()));
        }
        return v;
    }

    private final class CachedRecordReader implements Reader<E> {

        private final RandomAccessFile randomAccessFile;
        private E next;
        private AtomicLong position = new AtomicLong();

        public CachedRecordReader() throws IOException {
            this(0);
        }

        public CachedRecordReader(long position) throws IOException {
            this.randomAccessFile = new RandomAccessFile(file, "r");
            this.randomAccessFile.getChannel().position(position);
            advance();
        }

        public void setPosition(long position) throws IOException {
            randomAccessFile.getChannel().position(position);
        }

        @Override
        public long getPosition() throws IOException {
            return position.get();
        }

        @Override
        public void close() throws IOException {
            randomAccessFile.close();
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public E next() {
            if (!hasNext()) {
                return null;
            }
            try {
                final E result = next;
                advance();
                return result;
            } catch (IOException ex) {
                // put logging or something here
                throw new RuntimeException(ex);
            }
        }

        private void advance() throws IOException {
            FileChannel channel = randomAccessFile.getChannel();
            try {
                if (position.get() >= channel.size()) {
                    next = null;
                    return;
                }
                Record record = cache.get(position.get(), () -> read(channel.position(position.get())).get());
                next = record.getValue();
                position.set(record.getNextPosition());
            } catch (ExecutionException ex) {
                throw new IOException(ex);
            }
        }

    }

    private class CachedRecordWriter implements Writer<E> {

        private final FileChannel channel;
        private final ByteBuffer buffer;

        public CachedRecordWriter() throws IOException {
            channel = new RandomAccessFile(file, "rw").getChannel();
            buffer = ByteBuffer.allocate(16 << 20).order(ByteOrder.BIG_ENDIAN);
        }

        @Override
        public long append(E value) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            serializer.serialize(out, value);
            byte[] written = bytes.toByteArray();
            if (buffer.remaining() < written.length + 5) {
                sync();
            }
            ByteBufferOutputStream bufferOut = new ByteBufferOutputStream(buffer);
            bufferOut.write((byte) 'R');
            bufferOut.writeInt(written.length);
            bufferOut.write(written);
            long position = channel.position() + buffer.position();
            cache.put(position, new Record(value, written.length, position));
            return position;
        }

        @Override
        public void sync() throws IOException {
            buffer.flip();
            channel.write(buffer);
            buffer.clear();
        }

        @Override
        public void close() throws IOException {
            sync();
            channel.close();
        }
    }

    private class Record {
        private final E value;
        private final int size;
        private final long nextPosition;

        public Record(E value, int size, long nextPosition) {
            this.value = value;
            this.size = size;
            this.nextPosition = nextPosition;
        }

        public E getValue() {
            return value;
        }

        public int getSize() {
            return size;
        }

        public long getNextPosition() {
            return nextPosition;
        }
    }
}
