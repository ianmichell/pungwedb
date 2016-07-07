package com.pungwe.db.engine.io.volume;

import com.pungwe.db.engine.utils.Constants;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

// FIXME: This should be deprecated...
/**
 * Created by 917903 on 24/05/2016.
 */
public class MemoryMappedVolume extends ByteBufferVolume {

    private final RandomAccessFile randomAccessFile;

    public MemoryMappedVolume(String name, File file, boolean readOnly, int sliceShift)
            throws IOException {
        this(name, file, readOnly, sliceShift, -1l);
    }

    public MemoryMappedVolume(String name, File file, boolean readOnly, int sliceShift, long positionLimit)
        throws IOException {
        super(name, readOnly, sliceShift, positionLimit);
        this.randomAccessFile = new RandomAccessFile(file, readOnly ? "r" : "rw");
    }

    @Override
    public ByteBuffer makeNewBuffer(long offset) throws IOException {
        FileChannel channel = randomAccessFile.getChannel();
        FileLock lock = lock(channel, offset, offset + sliceSize);
        // Once we have a lok...
        try {
            ByteBuffer ret = channel.map(FileChannel.MapMode.READ_WRITE, offset, sliceSize);
            return ret;
        } finally {
            unlock(lock);
        }
    }

    private FileLock lock(FileChannel channel, long startOffset, long endOffset) throws IOException {
        FileLock lock = null;
        long time = System.currentTimeMillis();
        while ((lock = (endOffset == -1 ? channel.tryLock() : channel.tryLock(startOffset, endOffset, false))) == null) {
            // Sleep for 10 nanoseconds and try again
            try {
                Thread.sleep(0, Constants.LOCK_WAIT);
            } catch (InterruptedException e) {
            }

            // check the time.
            if (System.currentTimeMillis() -  time >= Constants.LOCK_TIMEOUT) {
                throw new IOException("Timed out whilst attempting to lock: "
                        + name() + " at position: " + startOffset);
            }
        }
        return lock;
    }

    private void unlock(FileLock lock) throws IOException {
        if (lock != null) {
            lock.close();
        }
    }

    @Override
    public boolean isClosed() {
        return !randomAccessFile.getChannel().isOpen();
    }

    @Override
    public void close() throws IOException {
        randomAccessFile.getChannel().close();
    }
}
