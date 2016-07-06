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

/**
 * Created by 917903 on 06/07/2016.
 */
public class CachedRecordFile<E> implements RecordFile<E> {

    private final Cache<Long, E> cache;
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
            return cache.get(position, () -> {
                FileChannel channel = new RandomAccessFile(file, "r").getChannel();
                try {
                    channel.position(position);
                    return read(channel).orElse(null);
                } finally {
                    channel.close();
                }
            });
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

    public Writer<E> writer() {
        return writer;
    }

    private Optional<E> read(FileChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(5).order(ByteOrder.BIG_ENDIAN);
        channel.read(buffer);
        buffer.flip();
        ByteBufferInputStream in = new ByteBufferInputStream(buffer);
        Optional<E> v = Optional.empty();
        E e = serializer.deserialize(in);
        if (e != null) {
            v = Optional.of(e);
        }
        channel.position(channel.position() + buffer.limit());
        return v;
    }

    private final class CachedRecordReader implements Reader<E> {

        private final RandomAccessFile randomAccessFile;
        private E next;

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
            return randomAccessFile.getChannel().position();
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
            }
            return null;
        }

        private void advance() throws IOException {
            FileChannel channel = randomAccessFile.getChannel();
            try {
                next = cache.get(channel.position(), () -> read(channel).orElse(null));
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
            if (buffer.remaining() < written.length) {
                sync();
            }
            buffer.put(written);
            return channel.position() + buffer.position();
        }

        @Override
        public void sync() throws IOException {
            buffer.flip();
            channel.write(buffer);
            buffer.clear();
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }
}
