package com.pungwe.db.engine.io.store;

import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.registry.SerializerRegistry;
import com.pungwe.db.engine.io.volume.Volume;
import com.pungwe.db.engine.utils.Constants;
import com.sun.tools.internal.jxc.ap.Const;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by 917903 on 24/05/2016.
 */
public class AppendOnlyStore implements Store {

    protected final Volume volume;
    protected final long maxEntries;
    protected Header header;
    protected final ReentrantReadWriteLock readWriteLock;

    public AppendOnlyStore(Volume volume) throws IOException {
        this(volume, -1l);
    }

    public AppendOnlyStore(Volume volume, long maxEntries) throws IOException {
        this.volume = volume;
        this.maxEntries = maxEntries;
        this.readWriteLock = new ReentrantReadWriteLock();
        if (volume.length() > 0) {
            header = findHeader();
        } else {
            header = new Header(this.getClass().getName(), Constants.BLOCK_SIZE, 0, 0, 0);
            // Ensure that the header is written, so that if we rollback, we can always find
            // the original
            writeHeader();
        }
    }

    protected long alloc(long size) throws IOException {
        int pages = (int) Math.ceil((double) size / header.getBlockSize());
        volume.ensureAvailable(header.getPosition() + size);
        long position = header.getNextPosition(pages * header.getBlockSize());
        return position;
    }

    @Override
    public Object get(long pointer, Serializer serializer) throws IOException {
        readWriteLock.readLock().lock();
        try {

            DataInput input = this.volume.getDataInput(pointer);
            byte type = input.readByte();
            switch (type) {
                case 'R':
                    break;
                default:
                    throw new IOException("Invalid record entry at offset: " + pointer);
            }
            input.skipBytes(20);
            return serializer.deserialize(input);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public long add(Object value, Serializer serializer) throws IOException {

        byte[] data = writeRecord(serializer, value);

        int length = data.length + 21; // get length of record being added
        int pages = (int) Math.ceil((double) length / header.getBlockSize());

        // We only want to lock the physcial position of the file to avoid seeking...
        readWriteLock.writeLock().lock();
        try {
            long previous = header.getPosition();
            long position = alloc(pages * header.getBlockSize());
            long next = header.getPosition();

            if (previous == position) {
                previous = -1;
            }

            // FIXME: We need a memory segment that this is written to, then pushed to disk on commit
            ByteBuffer buffer = ByteBuffer.allocate(21 + data.length);
            buffer.put((byte) 'R');
            buffer.putInt(data.length);
            buffer.putLong(previous);
            buffer.putLong(next);
            buffer.put(data);

            DataOutput output = this.volume.getDataOutput(position);
            output.write(buffer.array());

            // Incremenent record size
            header.incrementSize();

            return position;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public long update(long pointer, Object value, Serializer serializer)
            throws IOException {
        return add(value, serializer);
    }

    public void commit() throws IOException {
        writeHeader();
    }

    public void rollback() throws IOException {
        // Find the most recently written header
        header = findHeader();
    }

    @Override
    public long getPosition() {
        return header.getPosition();
    }

    @Override
    public long size() {
        return header.getSize();
    }

    private byte[] writeRecord(Serializer serializer, Object value) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteStream);
        try {
            serializer.serialize(out, value);
            // Return the array of bytes from the output stream.
            return byteStream.toByteArray();
        } finally {
            out.close();
            byteStream.close();
        }
    }

    @Override
    public boolean isAppendOnly() {
        return true;
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
        return findHeader(volume.length() - Constants.BLOCK_SIZE);
    }

    protected Header findHeader(final long position) throws IOException {
        // Lock the store whilst we try locate the most recent header.
        readWriteLock.readLock().lock();
        try {
            long current = position;
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
                    long offset = input.readLong(); // this is the last record written
                    long size = input.readLong();
                    return new Header(store, blockSize, firstOffset, offset, size);
                }
                current -= Constants.BLOCK_SIZE;
            }
            throw new IOException("Could not find store header. Volume could be wrong or corrupt");
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    protected void writeHeader() throws IOException {

        readWriteLock.writeLock().lock();
        try {
            long position = alloc(Constants.BLOCK_SIZE);

            // Write a byte array with the header...
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeByte((byte) 'H');
            out.writeUTF(header.getStore());
            out.writeInt(header.getBlockSize());
            out.writeLong(header.getFirstPosition());
            out.writeLong(header.getPosition());
            out.writeLong(header.getSize());

            byte[] headerBytes = bytes.toByteArray();

            DataOutput output = this.volume.getDataOutput(position);
            output.write(headerBytes);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void clear() throws IOException {
        // does nothing...
    }

    @Override
    public Volume getVolume() {
        return volume;
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

                ByteArrayInputStream bytesIn = new ByteArrayInputStream(buffer.array());
                DataInputStream dataIn = new DataInputStream(bytesIn);
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
        protected final AtomicLong currentIndex = new AtomicLong(0);
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
            return currentIndex.longValue() < header.getSize();
        }

        @Override
        public ByteBuffer next() {
            readWriteLock.readLock().lock();
            if (currentIndex.longValue() >= header.getSize()) {
                return null;
            }
            try {
                try {
                    // Check that the first thing we look at is not a header...
                    if (getPositionType(current.longValue()) == 'H') {
                        current.addAndGet(Constants.BLOCK_SIZE);
                    }
                    DataInput input = volume.getDataInput(current.get());
                    input.skipBytes(1);
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
                readWriteLock.readLock().unlock();
            }
        }

        @Override
        public void remove() {
            // FIXME: This can easily be implemented as a store is a giant linked list.
        }

        private void advance() throws IOException {
            long newPosition = findNextPosition(current.longValue());
            current.set(newPosition);
            currentIndex.incrementAndGet();
        }

        private long findNextPosition(long position) throws IOException {
            position = getNextPosition(position); // always run this first...
            if (position > header.getPosition()) {
                return position;
            }
            byte t = getPositionType(position);
            if (t == 'H') {
                return position + header.getBlockSize();
            }
            return position;
        }

        private byte getPositionType(long position) throws IOException {
            DataInput input = volume.getDataInput(position);
            byte t = input.readByte();
            return t;
        }

        private long getNextPosition(long position) throws IOException {
            DataInput input = volume.getDataInput(position);
            if (getPositionType(input.readByte()) == 'R') {
                input.skipBytes(13);
                return input.readLong();
            }
            return position + Constants.BLOCK_SIZE;
        }

    }

    protected static class Header {
        protected final String store;
        protected final int blockSize;
        protected final AtomicLong firstPosition = new AtomicLong(0);
        protected final AtomicLong nextPosition = new AtomicLong(0);
        protected AtomicLong size = new AtomicLong(0);
        protected AtomicLong metaData = new AtomicLong(0);

        public Header(String store, int blockSize, long firstPosition, long currentPosition, long size) {
            this.store = store;
            this.blockSize = blockSize;
            this.firstPosition.set(firstPosition);
            this.nextPosition.set(currentPosition);
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

        public void setFirstPosition(long position) {
            this.firstPosition.set(position);
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
