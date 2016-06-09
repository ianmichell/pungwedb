package com.pungwe.db.core.io.store;

import com.pungwe.db.core.utils.Constants;
import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.core.io.volume.Volume;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.ByteArrayInputStream;

/**
 * Created by 917903 on 24/05/2016.
 */
public class AppendOnlyStore extends DirectStore {

    public AppendOnlyStore(Volume volume) throws IOException {
        super(volume);
    }

    public AppendOnlyStore(Volume volume, long maxEntries) throws IOException {
        super(volume, maxEntries);
    }

    @Override
    public long update(long pointer, Object value, Serializer serializer)
        throws IOException {
        return add(value, serializer);
    }

    @Override
    protected Header findHeader() throws IOException {
        return findHeader(volume.length() - Constants.BLOCK_SIZE);
    }

    protected Header findHeader(final long position) throws IOException {
	    long current = position;
        while (current > 0) {
            byte[] buffer = new byte[Constants.BLOCK_SIZE];
            DataInput volumeInput = this.volume.getDataInput(current);
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
                long firstOffset = input.readLong();
                long offset = input.readLong(); // this is the last record written
                long size = input.readLong();
                return new Header(store, blockSize, firstOffset, offset, size);
            }
            current -= Constants.BLOCK_SIZE;
        }
        throw new IOException("Could not find store header. Volume could be wrong or corrupt");
    }

    @Override
    protected void writeHeader() throws IOException {

        long position = alloc(Constants.BLOCK_SIZE);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeByte((byte)'H');
        out.writeUTF(header.getStore());
        out.writeInt(header.getBlockSize());
        out.writeLong(header.getFirstPosition());
        out.writeLong(header.getPosition());
        out.writeLong(header.getSize());

        byte[] headerBytes = bytes.toByteArray();

        DataOutput output = this.volume.getDataOutput(position);
        output.write(headerBytes);
        if (Constants.BLOCK_SIZE - headerBytes.length > 0) {
            output.write(new byte[Constants.BLOCK_SIZE - headerBytes.length]);
        }
    }
}
