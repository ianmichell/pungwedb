package com.pungwe.db.engine.io.store;

import com.pungwe.db.engine.io.ByteBufferInputStream;
import com.pungwe.db.engine.io.ByteBufferOutputStream;
import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.registry.SerializerRegistry;
import com.pungwe.db.engine.io.volume.Volume;
import com.pungwe.db.engine.utils.Constants;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Append only store that wraps a volume and appends data to the end in log fashion
 */
public class BufferedRecordLogStore implements Store {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicLong bufferFilePosition = new AtomicLong();
    private final AtomicLong bufferRecords = new AtomicLong();
    private final AtomicInteger bufferWritePosition = new AtomicInteger(0);
    private final ByteBuffer buffer;
    protected final Volume volume;
    protected final long maxEntries;
    protected Header header;

    public BufferedRecordLogStore(Volume volume, int bufferSize, long maxEntries) throws IOException {
        this.volume = volume;
        this.maxEntries = maxEntries;
        // Create a cache build for the file
        this.buffer = ByteBuffer.allocate(bufferSize);
        if (volume.length() > 0) {
            header = findHeader();
            bufferFilePosition.set(header.getPosition());
        } else {
            header = new Header(this.getClass().getName(), Constants.BLOCK_SIZE, 0, 0, 0, 0);
            // Ensure that the header is written, so that if we rollback, we can always find
            // the original
            writeHeader();
            bufferFilePosition.set(getPosition());
        }
    }

    @Override
    public Object get(long pointer, Serializer serializer) throws IOException {
        lock.readLock().lock();
        int pos = (int)(pointer - bufferFilePosition.get());
        try {
            DataInput input = null;
            if (pointer >= bufferFilePosition.get()) {
                input = new ByteBufferInputStream((ByteBuffer)buffer.asReadOnlyBuffer().position(pos));
            } else {
                input = volume.getDataInput(pointer);
            }
            byte type = input.readByte();
            switch (type) {
                case 'R':
                    break;
                default:
                    throw new IOException("Invalid record entry at offset: " + pointer);
            }
            input.skipBytes(20);
            return serializer.deserialize(input);
        } catch (IllegalArgumentException ex) {
            // FIXME: Remove this and replace with something better, but it should not happen...
            ex.printStackTrace();
            throw ex;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long add(Object value, Serializer serializer) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        serializer.serialize(out, value);
        byte[] data = bytes.toByteArray();
        int length = data.length + 21;
        // We only want to lock the physcial position of the file to avoid seeking...
        lock.writeLock().lock();
        try {
            long previous = header.getPosition();
            long position = alloc(length);
            long next = header.getPosition();

            if (previous == position) {
                previous = -1;
            }

            // FIXME: We need a memory segment that this is written to, then pushed to disk on commit
            if (bufferWritePosition.get() == buffer.limit() || bufferWritePosition.get() + length > buffer.limit()) {
                flush();
            }

            // Create a write buffer
            ByteBuffer writeBuffer = buffer.duplicate();

            // Set the position of the write buffer to the current position
            writeBuffer.position(bufferWritePosition.get());

            // Create an output stream that data will be written to and write the data
            ByteBufferOutputStream outputStream = new ByteBufferOutputStream(writeBuffer);
            outputStream.writeByte((byte)'R');
            outputStream.writeInt(data.length); // 5
            outputStream.writeLong(previous); // 13
            outputStream.writeLong(next); // 21
            outputStream.write(data);

            // Set the last record position in the header
            header.setLastPosition(position);

            // Set the buffer position
            bufferWritePosition.set(writeBuffer.position());

            // Increment the overall record size
            header.incrementSize();
            // Increment the buffers record size
            bufferRecords.getAndIncrement();

            // return the position written to the file
            return position;
        } finally {
            lock.writeLock().unlock();
        }
    }

    protected long alloc(long size) throws IOException {
        volume.ensureAvailable(header.getPosition() + size);
        return header.getNextPosition(size);
    }

    private void flush() throws IOException {
        DataOutput out = volume.getDataOutput(bufferFilePosition.get());
        ByteBuffer flushBuffer = buffer.duplicate();
        out.write(flushBuffer.array(), 0, bufferWritePosition.get());
        bufferFilePosition.set(header.getPosition());
        buffer.clear();
        bufferRecords.set(0);
        bufferWritePosition.set(0);
    }

    @Override
    public long update(long pointer, Object value, Serializer serializer) throws IOException {
        return add(value, serializer);
    }

    @Override
    public void commit() throws IOException {
        lock.writeLock().lock();
        try {
            flush();
            writeHeader();
            // Update buffer position
            bufferFilePosition.set(header.getPosition());
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void rollback() throws IOException {
        lock.writeLock().lock();
        try {
            header = findHeader();
            buffer.clear();
            bufferFilePosition.set(header.getPosition());
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void clear() throws IOException {
        // FIXME: Do something here
    }

    @Override
    public long size() {
        return header.getSize();
    }

    @Override
    public Volume getVolume() {
        return this.volume;
    }

    @Override
    public long getPosition() {
        return header.getPosition();
    }

    @Override
    public Iterator<Object> iterator() {
        try {
            return new RecordIterator();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected Header findHeader() throws IOException {
        long position = volume.length();
        if (position > header.getBlockSize() && (position % header.getBlockSize()) > 0) {
            position = position - (position % header.getBlockSize());
        } else {
            position = position - Constants.BLOCK_SIZE;
        }
        return findHeader(position);
    }

    protected Header findHeader(final long position) throws IOException {
        // Lock the store whilst we try locate the most recent header.
        lock.readLock().lock();
        try {
            long current = position < 0 ? 0 : position;
            while (current >= 0) {
                byte[] buffer = new byte[Constants.BLOCK_SIZE];
                DataInput volumeInput = this.volume.getDataInput(current);
                volumeInput.readFully(buffer);
                byte firstByte = buffer[0];
                if (firstByte == (byte) 'H') {
                    ByteArrayInputStream bytes = new ByteArrayInputStream(buffer);
                    DataInputStream input = new DataInputStream(bytes);
                    input.skip(1); // Skip the first byte
                    String store = input.readUTF();
                    if (store == null || !store.equals(this.getClass().getName())) {
                        throw new IOException("Volume is not of the correct format for this store");
                    }
                    int blockSize = input.readInt();
                    long firstOffset = input.readLong();
                    long lastOffset = input.readLong();
                    long offset = input.readLong(); // this is the current offset
                    long size = input.readLong();
                    return new Header(store, blockSize, firstOffset, offset, lastOffset, size);
                }
                current -= Constants.BLOCK_SIZE;
            }
            throw new IOException("Could not find store header. Volume could be wrong or corrupt");
        } finally {
            lock.readLock().unlock();
        }
    }

    protected void writeHeader() throws IOException {

        lock.writeLock().lock();
        try {
            // Get current position
            long position = header.getPosition();

            if (position > 0 && position < header.getBlockSize()) {
                alloc(header.getBlockSize() - position);
            } else if ((position % header.getBlockSize()) > 0) {
                alloc(header.getBlockSize() - (position % header.getBlockSize()));
            }
            position = alloc(header.getBlockSize());

            // Write a byte array with the header...
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeByte((byte) 'H');
            out.writeUTF(header.getStore());
            out.writeInt(header.getBlockSize());
            out.writeLong(header.getFirstPosition());
            out.writeLong(header.getLastPosition());
            out.writeLong(header.getPosition());
            out.writeLong(header.getSize());

            // Maximum size has to be
            byte[] headerBytes = Arrays.copyOf(bytes.toByteArray(), header.getBlockSize());

            DataOutput dataOut = volume.getDataOutput(position);
            dataOut.write(headerBytes);

        } finally {
            lock.writeLock().unlock();
        }
    }

    protected class RecordIterator implements Iterator<Object> {

        private final Iterator<ByteBuffer> it;

        public RecordIterator() throws IOException {
            it = new DataIterator();
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public Object next() {
            if (!hasNext()) {
                return null;
            }
            try {
                ByteBuffer buffer = it.next();
                // Double check to ensure buffer isn't null...
                if (buffer == null) {
                    return null;
                }

                ByteBufferInputStream dataIn = new ByteBufferInputStream(buffer.asReadOnlyBuffer());
                String serializerKey = dataIn.readUTF();
                Serializer serializer = SerializerRegistry.getIntance().getByKey(serializerKey);
                if (serializer == null) {
                    throw new IllegalArgumentException("Could not find appropriate serializer");
                }
                dataIn.reset();
                return serializer.deserialize(dataIn);
            } catch (IOException ex) {
                // FIXME: Add logging...
                return null;
            }
        }

        @Override
        public void remove() {
            // FIXME: This can easily be implemented as a store is a giant linked list.
        }
    }

    protected class DataIterator implements Iterator<ByteBuffer> {

        protected final AtomicLong current = new AtomicLong(-1l);
        protected final Header header;

        public DataIterator() throws IOException {
            // Fetch the most recently written header.
            // The iterator is classified as a point in time
            // of the store. We do not want new records to affect
            // it.
            this.header = findHeader();
            current.set(header.getFirstPosition());
        }

        @Override
        public boolean hasNext() {
            // Check that the current value is less than the position minus the header size...
            return current.get() <= header.getLastPosition();
        }

        @Override
        public ByteBuffer next() {
            lock.readLock().lock();
            try {
                try {
                    if (!hasNext()) {
                        return null;
                    }
                    // Check that the first thing we look at is not a header...
                    if (getPositionType(current.longValue()) != 'R') {
                        advance();
                    }
                    DataInput input = volume.getDataInput(current.get());
                    byte type = input.readByte();
                    assert type == (byte)'R';
                    int length = input.readInt();
                    input.skipBytes(16);
                    byte[] buffer = new byte[length];
                    input.readFully(buffer);
                    ByteBuffer data = ByteBuffer.wrap(buffer);
                    advance(); // Go to the next record
                    return data;
                } catch (IOException ex) {
                    return null;
                }
            } finally {
                lock.readLock().unlock();
            }
        }

        @Override
        public void remove() {
            // FIXME: This can easily be implemented as a store is a giant linked list.
        }

        private void advance() throws IOException {
            long newPosition = findNextPosition(current.longValue());
            current.set(newPosition);
        }

        private long findNextPosition(final long position) throws IOException {
            byte t = 0;
            long current = position;
            while (t != 'R' && current < header.getLastPosition()) {
                current = getNextPosition(current);
                if (current > header.getPosition()) {
                    return current;
                }
                t = getPositionType(current);
            }
            return current;
        }

        private long getNextPosition(final long position) throws IOException {
            DataInput input = volume.getDataInput(position);
            byte t = getPositionType(position);
            if (t == 'R') {
                input.skipBytes(13);
                return input.readLong();
            } else if (t == (byte)'H') {
                return position + header.getBlockSize();
            }
            // FIXME: We should not be here... But we will scan one byte at a time until we find a record or header
            return position + 1;
        }

        private byte getPositionType(long position) throws IOException {
            DataInput input = volume.getDataInput(position);
            byte t = input.readByte();
            return t;
        }
    }

    protected static class Header {
        protected final String store;
        protected final int blockSize;
        protected final AtomicLong lastPosition = new AtomicLong(0);
        protected final AtomicLong firstPosition = new AtomicLong(0);
        protected final AtomicLong nextPosition = new AtomicLong(0);
        protected AtomicLong size = new AtomicLong(0);
        protected AtomicLong metaData = new AtomicLong(0);

        public Header(String store, int blockSize, long firstPosition, long currentPosition, long lastPosition, long size) {
            this.store = store;
            this.blockSize = blockSize;
            this.firstPosition.set(firstPosition);
            this.nextPosition.set(currentPosition);
            this.lastPosition.set(lastPosition);
            this.size.set(size);
        }

        public void setPosition(long position) {
            this.nextPosition.set(position);
        }

        public long getPosition() {
            return this.nextPosition.longValue();
        }

        public long getNextPosition(long size) {
            return nextPosition.getAndAdd(size);
        }

        public long getFirstPosition() {
            return firstPosition.longValue();
        }

        public long getLastPosition() {
            return lastPosition.get();
        }

        public void setLastPosition(long position) {
            this.lastPosition.set(position);
        }

        public String getStore() {
            return store;
        }

        public int getBlockSize() {
            return blockSize;
        }

        public long getSize() {
            return size.get();
        }

        public void setSize(long size) {
            this.size.set(size);
        }

        public long incrementSize() {
            return this.size.incrementAndGet();
        }
    }
}
