package com.pungwe.db.core.io.volume;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * Created by 917903 on 24/05/2016.
 */
public class HeapByteBufferVolume extends ByteBufferVolume {

    private boolean closed;
    private long currentSize;

    public HeapByteBufferVolume(String name, boolean readOnly, int sliceShift, long maxSize) {
        super(name, readOnly, sliceShift, maxSize);
    }

    @Override
    public ByteBuffer makeNewBuffer(long offset) throws IOException {
        try {
            lock();
            int size = sliceSize;
            // FIXME: This is going to cause problems...
            if (positionLimit > 0) {
                // Maximum 2GB... Integer.MAX_VALUE... Would be lovely if the JVM could
                // address more... :(
                size = (int)Math.min(sliceSize, positionLimit - currentSize);
            }
            ByteBuffer buffer = ByteBuffer.allocate(size);
            currentSize = size;
            return buffer;
        } catch (OutOfMemoryError ex) {
            throw ex;
        } finally {
            unlock();
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() throws IOException {
        slices = null;
        closed = true;
    }
}
