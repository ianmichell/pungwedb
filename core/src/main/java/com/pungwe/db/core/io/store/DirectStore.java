package com.pungwe.db.core.io.store;

import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.core.io.volume.Volume;
import com.pungwe.db.core.utils.Constants;

import java.util.concurrent.atomic.AtomicLong;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Iterator;


/**
 * Created by ian on 27/05/2016.
 */
public class DirectStore implements Store {

    protected final Volume volume;
    protected final long maxEntries;
    protected final Header header;
    protected Iterator<Object> it;

    public DirectStore(Volume volume) throws IOException {
        this(volume, -1l);
    }

    public DirectStore(Volume volume, long maxEntries) throws IOException {
        this.volume = volume;
        this.maxEntries = maxEntries;
        if (volume.length() > 0) {
            header = findHeader();
        } else {
            header = new Header(this.getClass().getName(), Constants.BLOCK_SIZE);
        }
    }

    protected Header findHeader() throws IOException {
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
    }

    protected void writeHeader() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeByte((byte)'H');
        out.writeUTF(header.getStore());
        out.writeInt(header.getBlockSize());
        out.writeLong(header.getFirstPosition());
        out.writeLong(header.getPosition());
        out.writeLong(header.getSize());

        byte[] headerBytes = bytes.toByteArray();

        DataOutput output = this.volume.getDataOutput(0);
        output.write(headerBytes);
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
    }

    @Override
    public long add(Object value, Serializer serializer) throws IOException {
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
        int remainder = ((pages * Constants.BLOCK_SIZE) - data.length) - 21;
        if (remainder > 0) {
            output.write(new byte[remainder]);
        }

        // Incremenent record size
        header.incrementSize();

        writeHeader();

        return position;
    }

    @Override
    public long update(final long pointer, Object value, Serializer serializer) throws IOException {

        long current = pointer;

        DataInput input = this.volume.getDataInput(current);

        byte b = input.readByte(); // Read the first byte...
        switch (b) {
            case 'R':
                // Do nothing!
                break;
            case 'M': {
                input.skipBytes(8);
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
    public Iterator<Object> iterator() {
        if (it == null) {
            it = new RecordIterator();
        }
        return it;
    }

    protected class RecordIterator implements Iterator<Object> {

        protected final AtomicLong current = new AtomicLong(-1l);

        public RecordIterator() {
            current.set(header.getFirstPosition());
        }

        @Override
        public boolean hasNext() {
            return current.longValue() <= header.getPosition();
        }

        @Override
        public Object next() {
            if (current.longValue() > header.getPosition()) {
                return null;
            }
            try {
                // FIXME: Add a serializer registry
                Object value = get(current.longValue(), new ObjectSerializer());
                advance(); // Go to the next record
                return value;
            } catch (IOException ex) {
                return null;
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
            this(store, blockSize, blockSize);
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
