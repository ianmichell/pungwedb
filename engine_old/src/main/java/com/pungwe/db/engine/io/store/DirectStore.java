package com.pungwe.db.engine.io.store;

import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.core.registry.SerializerRegistry;
import com.pungwe.db.engine.io.volume.Volume;
import com.pungwe.db.engine.utils.Constants;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.ByteArrayInputStream;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Created by ian on 27/05/2016.
 */
public class DirectStore implements Store {

    protected final Volume volume;
    protected final long maxEntries;
    protected Header header;
    protected final ReentrantReadWriteLock readWriteLock;

    public DirectStore(Volume volume) throws IOException {
        this(volume, -1l);
    }

    public DirectStore(Volume volume, long maxEntries) throws IOException {
        this(volume, maxEntries, Constants.BLOCK_SIZE);
    }

    protected DirectStore(Volume volume, long maxEntries, long firstRecordPosition) throws IOException {
        this.volume = volume;
        this.maxEntries = maxEntries;
        this.readWriteLock = new ReentrantReadWriteLock();
        if (volume.length() > 0) {
            header = findHeader();
        } else {
            header = new Header(this.getClass().getName(), Constants.BLOCK_SIZE, firstRecordPosition);
            writeHeader();
        }
    }

    protected Header findHeader() throws IOException {
        readWriteLock.readLock().lock();
        try {
            byte[] buffer = new byte[Constants.BLOCK_SIZE];
            DataInput volumeInput = this.volume.getDataInput(0l);
            volumeInput.readFully(buffer);
            byte firstByte = buffer[0];
            if (firstByte == (byte)'H') {
                ByteArrayInputStream bytes = new ByteArrayInputStream(buffer);
                DataInputStream input = new DataInputStream(bytes);
                input.skip(1); // Skip the first byte
                String store = input.readUTF();
                if (store == null || !store.equals(this.getClass().getName())) {
                    throw new IOException("Volume is not of the correct format for this store");
                }
                int blockSize = input.readInt();
                long firstPosition = input.readLong();
                long position = input.readLong(); // this is the last record written
                long size = input.readLong();
                return new Header(store, blockSize, firstPosition, position, size);
            }
            throw new IOException("Could not find the header in this volume - " + volume.name());
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    protected void writeHeader() throws IOException {
        writeHeader(0);
    }

    protected void writeHeader(long position) throws IOException {
        readWriteLock.writeLock().lock();
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeByte((byte)'H');
            out.writeUTF(header.getStore());
            out.writeInt(header.getBlockSize());
            out.writeLong(header.getFirstPosition());
            out.writeLong(header.getPosition());
            out.writeLong(header.getSize());

            byte[] headerBytes = bytes.toByteArray();

            this.volume.ensureAvailable(position);
            DataOutput output = this.volume.getDataOutput(position);
            output.write(headerBytes);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    protected Header getHeader() throws IOException {
        return header;
    }

    protected long alloc(long size) throws IOException {
        int pages = (int) Math.ceil((double) size / header.getBlockSize());
        long position = header.getNextPosition(pages * header.getBlockSize());
        return position;
	}

    @Override
    public Object get(long pointer, Serializer serializer) throws IOException {
        readWriteLock.readLock().lock();
        try {
            DataInput input = this.volume.getDataInput(pointer);
            byte type = input.readByte();
            int length = input.readInt();
            switch (type) {
                case 'R':
                    break;
                case 'M': {
                    input.skipBytes(8);
                    return this.get(input.readLong(), serializer);
                }
                case 'D':
                    throw new IOException("Record at position: " + pointer + " has been deleted!");
                default:
                    throw new IOException("Invalid record entry at offset: " + pointer);
            }
            long previousRecord = input.readLong();
            long nextRecord = input.readLong();
            return serializer.deserialize(input);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public long add(Object value, Serializer serializer) throws IOException {
        readWriteLock.writeLock().lock();
        try {
            byte[] data = writeRecord(serializer, value);

            int length = data.length + 21; // get length of record being added
            int pages = (int)Math.ceil((double)length / header.getBlockSize());

            long previous = header.getPosition();
            long position = alloc(pages * header.getBlockSize());
            long next = position + (pages * header.getBlockSize());

            if (previous == position) {
                previous = -1;
            }

            // Ensure we have the space available...
            this.volume.ensureAvailable(position + (header.getBlockSize() * pages));

            // Write the record...
            DataOutput output = this.volume.getDataOutput(position);
            output.writeByte('R');
            output.writeInt(data.length);
            // Previous record
            output.writeLong(previous);
            // Next record
            output.writeLong(next);
            output.write(data);

            // Calculate remainder of block
//            int remainder = ((pages * Constants.BLOCK_SIZE) - data.length) - 21;
//            if (remainder > 0) {
//                output.write(new byte[remainder]);
//            }

            // Incremenent record size
            header.incrementSize();

            writeHeader();

            return position;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public long update(final long pointer, Object value, Serializer serializer) throws IOException {

        readWriteLock.writeLock().lock();
        try {
            long current = pointer;
            DataInput input = this.volume.getDataInput(current);

            byte b = input.readByte(); // Read the first byte...
            switch (b) {
                case 'R':
                    // Do nothing!
                    break;
                case 'M': {
                    input.skipBytes(12);
                    current = input.readLong();
                    input = this.volume.getDataInput(current);
                    break;
                }
                case 'D': {
                    throw new IOException("Offset: " + current + " was deleted!");
                }
                default:
                    throw new IOException("Offset: " + current + " is not a valid record");
            }
            // Existing page size...
            int size = input.readInt();
            int pages = (int)Math.ceil((double)(size + 21) / header.getBlockSize());

            long previousRecord = input.readLong();
            long nextRecord = input.readLong();

            // Write a byte array for the new record...
            byte[] bytes = writeRecord(serializer, value);

            // Calculate the new page size
            int newPageSize = (int)Math.ceil((double)(bytes.length + 21) / header.getBlockSize());

            // FIXME: Check padding...
            if (newPageSize > pages) {
                // If the existing size is smaller than the document append the the end of the volume...
                long newPosition = add(value, serializer);
                // We need to blank out the existing record and point it to the new one.
                DataOutput output = this.volume.getDataOutput(current);
                output.writeByte((byte)'M');
                output.writeInt(0);
                output.writeLong(previousRecord);
                output.writeLong(newPosition);

                return newPosition;
            }
            // Update the existing record
            DataOutput output = this.volume.getDataOutput(current);
            output.writeByte('R');
            output.writeInt(bytes.length);
            output.writeLong(previousRecord);
            output.writeLong(nextRecord);
            output.write(bytes);
            // Calculate remainder of block
            int remainder = ((newPageSize * Constants.BLOCK_SIZE) - bytes.length) - 21;
            if (remainder > 0) {
                output.write(new byte[remainder]);
            }
            return current;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public void commit() throws IOException {
        // do nothing on direct store
    }

    public void rollback() throws IOException {
        // do nothing on direct store
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

    /**
     * Resets the position of the store to the beginning
     *
     * @throws IOException
     */
    @Override
    public void clear() throws IOException {
        this.getHeader().setPosition(this.getHeader().getFirstPosition());
    }

    @Override
    public Volume getVolume() {
        return volume;
    }

    @Override
    public long getPosition() {
        return header.getPosition();
    }


    // FIXME: We need to use the registry
    @Override
    public Iterator<Object> iterator() {
        try {
            return new RecordIterator();
        } catch (IOException ex) {
            // should never happen
            return null;
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
    }

    protected class DataIterator implements Iterator<ByteBuffer> {
        protected final AtomicLong current = new AtomicLong(-1l);

        public DataIterator() {
            current.set(header.getFirstPosition());
        }

        @Override
        public boolean hasNext() {
            return current.longValue() <= header.getPosition();
        }

        @Override
        public ByteBuffer next() {
            readWriteLock.readLock().lock();
            try {
                if (current.longValue() >= header.getPosition()) {
                    return null;
                }
                try {
                    // Check that the first thing we look at is not a header...
                    if (getPositionType(current.longValue()) == 'H') {
                        current.addAndGet(Constants.BLOCK_SIZE);
                    }
                    DataInput input = volume.getDataInput(current.get());
                    input.skipBytes(17);
                    int length = input.readInt();
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
        }

        private long findNextPosition(long position) throws IOException {
            position = getNextPosition(position); // always run this first...
            if (position > header.getPosition()) {
                return position;
            }
            byte t = getPositionType(position);
            if (t == 'H') {
                return findNextPosition(position + header.getBlockSize());
            } else if (t != 'R') {
                return findNextPosition(getNextPosition(position));
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
            input.skipBytes(13);
            return input.readLong();
        }
    }

    protected static class Header {
        protected final String store;
        protected final int blockSize;
        protected final AtomicLong firstPosition = new AtomicLong(0);
        protected final AtomicLong nextPosition = new AtomicLong(0);
        protected AtomicLong size = new AtomicLong(0);

        public Header(String store, int blockSize) {
            this(store, blockSize, 0);
        }

        public Header(String store, int blockSize, long currentPosition) {
            this(store, blockSize, currentPosition, 0);
        }

        public Header(String store, int blockSize, long currentPosition, long size) {
            this(store, blockSize, blockSize, currentPosition, size);
        }

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