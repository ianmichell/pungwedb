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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ian on 07/07/2016.
 */
public class BasicRecordFile<E> implements RecordFile<E> {

    protected final Serializer<E> serializer;
    protected final File file;
    protected Map<String, Object> metaData;
    protected BasicRecordFileWriter writer;

    public BasicRecordFile(File file, Serializer<E> serializer) throws IOException {
        this.file = file;
        this.serializer = serializer;
        if (file.length() > 0) {
            getMetaData(); // Load the metadata
            this.writer = new BasicRecordFileWriter(file.length());
        } else {
            this.writer = new BasicRecordFileWriter(0);
        }
    }

    @Override
    public E get(long position) throws IOException {
        Reader<E> reader = reader(position);
        if (reader.hasNext()) {
            return reader.next();
        }
        // FIXME: We might want to throw an io error here...
        return null;
    }

    @Override
    public void setMetaData(Map<String, Object> metaData) {
        if (this.metaData == null) {
            // Create a new meta data.
            this.metaData = new LinkedHashMap<>();
        }
        this.metaData.putAll(metaData);
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
        // This could be a meta data entry...
        if (t != 'R') {
            channel.position(channel.position() + 4095);
            return read(channel);
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
                    // If M is not the first byte, then read back another 4k.
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
            } finally {
                channel.close();
            }
        }
        throw new IOException("Could not find meta data");
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
        return null;
    }

    @Override
    public Iterator<E> iterator() {
        return null;
    }

    private class BasicRecordFileReader implements Reader<E> {

        private final RandomAccessFile randomAccessFile;
        private E next;
        private AtomicLong position = new AtomicLong();

        private BasicRecordFileReader(long position) throws IOException {
            randomAccessFile = new RandomAccessFile(file, "r");
            this.randomAccessFile.getChannel().position(position);
            advance();
        }

        private void advance() throws IOException {
            FileChannel channel = randomAccessFile.getChannel();
            if (position.get() >= channel.size()) {
                next = null;
                return;
            }
            Optional<Record> record = read(channel.position(position.get()));
            if (!record.isPresent()) {
                next = null; // nothing left...
                return;
            }
            next = record.get().getValue();
            position.set(record.get().getNextPosition());
        }

        @Override
        public long getPosition() throws IOException {
            return randomAccessFile.getChannel().position();
        }

        @Override
        public void setPosition(long position) throws IOException {
            randomAccessFile.getChannel().position(position);
        }

        @Override
        public void close() throws IOException {
            randomAccessFile.getChannel().close();
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
    }

    private class BasicRecordFileWriter implements Writer<E> {

        private final FileChannel channel;
        private final ByteBuffer buffer;

        public BasicRecordFileWriter() throws IOException {
            this(0);
        }

        public BasicRecordFileWriter(long position) throws IOException {
            this.channel = new RandomAccessFile(file, "rw").getChannel();
            if (position > 0) {
                channel.position(position);
            }
            // 16MB buffer...
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
            commit();
            channel.close();
        }

        public void commit() throws IOException {
            sync();
            // Write the meta data
            int remaining = (int)(channel.position() > 4096 ? channel.position() % 4096 : 4096 - channel.position());
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
        }
    }

    private class Record {
        private final E value;
        private final int size;
        private final long nextPosition;

        public Record(E value, int size, long position) {
            this.value = value;
            this.size = size;
            nextPosition = position;
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
