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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.engine.io.util.ByteBufferOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * Created by ian on 22/07/2016.
 */
public class CachingRecordFile<E> extends BasicRecordFile<E> {

    // FIXME: Add a proper cache manager here...
    private final Cache<Long, Record<E>> cache;

    public CachingRecordFile(File file, Serializer<E> serializer, int cacheSize) throws IOException {
        super(file, serializer);
        cache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
        writer = new CachingRecordWriter();
    }

    @Override
    public E get(long position) throws IOException {
        try {
            Record<E> record = cache.get(position, () -> {
                FileChannel channel = raf.getChannel();
                // FIXME: We might want to throw an io error here...
                Optional<Record<E>> r = read(channel, position);
                return r.orElse(null);
            });
            return record != null ? record.getValue() : null;
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Reader<E> reader(long position) throws IOException {
        return new CachedRecordFileReader(position);
    }

    private class CachedRecordFileReader extends BasicRecordFileReader {

        public CachedRecordFileReader(long position) throws IOException {
            super(position);
        }

        protected void advance() throws IOException {
            final FileChannel channel = raf.getChannel();
            if (position.get() >= channel.size()) {
                next = null;
                return;
            }
            try {
                Record<E> record = cache.get(position.get(), () -> {
                    // FIXME: We might want to throw an io error here...
                    Optional<Record<E>> r = read(channel, position.get());
                    return r.orElse(null);
                });
                if (record == null) {
                    next = null;
                    return;
                }
                next = record;
                position.set(record.getNextPosition());
            } catch (ExecutionException e) {
                throw new IOException(e);
            }
        }
    }

    private class CachingRecordWriter extends BasicRecordFileWriter {

        public CachingRecordWriter() throws IOException {
            this(0);
        }

        public CachingRecordWriter(long position) throws IOException {
            super(position);
        }

        @Override
        public synchronized long append(E value) throws IOException {
            lock.lock();
            try {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes);
                serializer.serialize(out, value);
                byte[] written = bytes.toByteArray();
                if (buffer.remaining() < written.length + 5) {
                    sync();
                }
                long position = channelPosition.get() + buffer.position();
                ByteBufferOutputStream bufferOut = new ByteBufferOutputStream(buffer);
                bufferOut.write((byte) 'R');
                bufferOut.writeInt(written.length);
                bufferOut.write(written);
                this.position.set(position + buffer.position());
                cache.put(position, new Record<>(value, written.length + 5, position, this.position.get()));
                return position;
            } finally {
                lock.unlock();
            }
        }
    }
}
