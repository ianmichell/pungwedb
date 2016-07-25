package com.pungwe.db.engine.io;

import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.engine.io.util.ByteBufferInputStream;
import com.pungwe.db.engine.io.util.ByteBufferOutputStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by ian when 07/07/2016.
 */
public class BasicRecordFile<E> implements RecordFile<E> {

    protected final ReentrantLock lock = new ReentrantLock();
    protected final AtomicBoolean closed = new AtomicBoolean();
    protected final Serializer<E> serializer;
    protected final File file;
    protected final RandomAccessFile raf;
    protected Map<String, Object> metaData;
    protected BasicRecordFileWriter writer;

    public BasicRecordFile(File file, Serializer<E> serializer) throws IOException {
        this.file = file;
        raf = new RandomAccessFile(file, "rw");
        this.serializer = serializer;
        if (file.length() > 0) {
            getMetaData(); // Load the metadata
            this.writer = new BasicRecordFileWriter(file.length());
        } else {
            this.writer = new BasicRecordFileWriter(0);
        }
    }

    @Override
    public String getPath() {
        return file.getAbsolutePath();
    }

    @Override
    public E get(long position) throws IOException {
        FileChannel channel = raf.getChannel();
        // FIXME: We might want to throw an io error here...
        Optional<Record<E>> record = read(channel, position);
        if (!record.isPresent()) {
            return null;
        }
        return record.get().getValue();
    }

    @Override
    public long size() throws IOException {
        return writer.getPosition();
    }

    @Override
    public void setMetaData(Map<String, Object> metaData) {
        if (this.metaData == null) {
            // Create a new meta data.
            this.metaData = new LinkedHashMap<>();
        }
        this.metaData.putAll(metaData);
    }

    @Override
    public boolean delete() throws IOException {
        // Close the tree
        closed.set(true);
        // delete the files
        raf.close();
        return file.delete();
    }

    protected Optional<Record<E>> read(FileChannel channel, long position) throws IOException {
        lock.lock();
        try {
            if (position + 5 >= channel.size()) {
                return Optional.empty();
            }
            channel = channel.position(position);
            ByteBuffer buffer = ByteBuffer.wrap(new byte[5]).order(ByteOrder.BIG_ENDIAN);
            channel.read(buffer);
            buffer.flip();
            ByteBufferInputStream in = new ByteBufferInputStream(buffer.asReadOnlyBuffer());
            // Ensure what we have call a record
            byte t = in.readByte();
            // This could be a meta data entry...
            if (t == (byte) 'M') {
                long newPosition = position + 4096;
                return read(channel, newPosition);
            } else if (t != (byte) 'R') {
                // Shift through the channel 1 byte at a time... We can't find the record.
                long newPosition = position + 1;
                return read(channel, newPosition);
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
            Optional<Record<E>> v = Optional.empty();
            // deserialize the record into an object.
            E e = serializer.deserialize(in);
            if (e != null) {
                v = Optional.of(new Record<>(e, length, position, channel.position()));
            }
            return v;
        } finally {
            lock.unlock();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> getMetaData() throws IOException {
        if (metaData == null) {
            if (!file.exists()) {
                file.createNewFile();
            }
            if(file.length() == 0) {
                metaData = new LinkedHashMap<>();
                return metaData;
            }
            // We need to read the file 4KB at a time to find the meta data start point.
            long position = file.length() - 4096;
            FileChannel channel = new RandomAccessFile(file, "r").getChannel();
            try {
                while (position >= 0) {
                    channel.position(position);
                    ByteBuffer buffer = ByteBuffer.allocate(4096);
                    channel.read(buffer);
                    ByteBufferInputStream input = new ByteBufferInputStream(buffer.asReadOnlyBuffer());
                    // If M call not the first byte, then read back another 4k.
                    if (input.readByte() != 'M') {
                        position -= 4096;
                        continue;
                    }
                    // Get the size!
                    int size = input.readInt();
                    // Read the whole thing in.
                    buffer = ByteBuffer.allocate(size);
                    input = new ByteBufferInputStream(buffer);
                    input.skipBytes(5); // We don't care about the first 5 bytes
                    metaData = (Map<String, Object>)new ObjectSerializer().deserialize(input);
                    return metaData;
                }
                // No meta data found... We need a new one
                metaData = new LinkedHashMap<>();
                return metaData;
            } finally {
                channel.close();
            }
        }
        return metaData;
    }

    @Override
    public Reader<E> reader() throws IOException {
        return new BasicRecordFileReader(0);
    }

    @Override
    public Reader<E> reader(long position) throws IOException {
        return new BasicRecordFileReader(position);
    }

    @Override
    public Writer<E> writer() throws IOException {
        return writer;
    }

    @Override
    public Iterator<E> iterator() {
        try {
            return new BasicRecordFileReader(0l);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected class BasicRecordFileReader implements Reader<E> {

        protected Record<E> next;
        protected AtomicLong position = new AtomicLong();

        protected BasicRecordFileReader(long position) throws IOException {;
            this.position.set(position);
            advance();
        }

        protected void advance() throws IOException {
            FileChannel channel = raf.getChannel();
            if (position.get() >= channel.size()) {
                next = null;
                return;
            }
            Optional<Record<E>> record = read(channel, position.get());
            if (!record.isPresent()) {
                next = null; // nothing left...
                return;
            }
            next = record.get();
            position.set(record.get().getNextPosition());
        }

        @Override
        public long getPosition() throws IOException {
            return position.get();
        }

        @Override
        public void setPosition(long position) throws IOException {
            this.position.set(position);
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public E next() {
            Record<E> v = nextRecord();
            return v == null ? null : v.getValue();
        }

        @Override
        public Record<E> nextRecord() {
            if (!hasNext()) {
                return null;
            }
            try {
                if (next == null) {
                    return null;
                }
                final Record<E> result = next;
                advance();
                return result;
            } catch (IOException ex) {
                // put logging or something here
                throw new RuntimeException(ex);
            }
        }
    }

    protected class BasicRecordFileWriter implements Writer<E> {

        protected final FileChannel channel;
        protected final ByteBuffer buffer;
        protected final AtomicLong position = new AtomicLong();
        protected final AtomicLong channelPosition = new AtomicLong();

        public BasicRecordFileWriter() throws IOException {
            this(0);
        }

        public BasicRecordFileWriter(long position) throws IOException {
            this.channel = raf.getChannel();
            if (position > 0) {
                this.channelPosition.set(position);
                this.position.set(position);
            }
            // 16MB buffer...
            buffer = ByteBuffer.allocate(16 << 20).order(ByteOrder.BIG_ENDIAN);
        }

        @Override
        public long getPosition() throws IOException {
            return position.get();
        }

        public long getFileSize() throws IOException {
            return channelPosition.get();
        }

        public int calculateWriteSize(E value) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(bytes);
            dataOut.write((byte)'R');
            dataOut.writeInt(0);
            serializer.serialize(dataOut, value);
            return bytes.toByteArray().length;
        }

        @Override
        public long append(E value) throws IOException {
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
                return position;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void sync() throws IOException {
            lock.lock();
            try {
                buffer.flip();
                channel.position(channelPosition.get()).write(buffer);
                buffer.clear();
                // Update channel position and buffer position.
                channelPosition.set(channel.position());
                position.set(channelPosition.get());
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void close() throws IOException {
            commit();
            channel.close();
        }

        public void commit() throws IOException {
            lock.lock();
            try {
                sync();
                // If we have no meta data, don't bother writing any...
                if (metaData == null || metaData.isEmpty()) {
                    return;
                }
                // Write the meta data
                int remaining = (int) (position.get() > 4096 ? position.get() % 4096 : 4096 - position.get());
                ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
                DataOutputStream dataOut = new DataOutputStream(bytesOut);
                // Write the object to data out
                new ObjectSerializer().serialize(dataOut, metaData);
                byte[] meta = bytesOut.toByteArray();
                ByteBufferOutput byteBufferOutput = new ByteBufferOutput(buffer);
                // Write empty bytes up to remaining
                if (remaining > 0) {
                    byteBufferOutput.write(new byte[remaining]);
                }
                byteBufferOutput.writeByte('M');
                byteBufferOutput.writeInt(meta.length);
                byteBufferOutput.write(meta);
                sync();
            } finally {
                lock.unlock();
            }
        }
    }
}
