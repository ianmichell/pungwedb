package com.pungwe.db.engine.io.volume;

import com.pungwe.db.core.utils.ConfigSingleton;
import com.pungwe.db.engine.utils.Constants;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by 917903 on 23/05/2016.
 */
public final class RandomAccessVolume implements Volume {

    private final String name;
    private RandomAccessFile randomAccessFile;
    private final boolean readOnly;
    private final long positionLimit;

    public RandomAccessVolume(File file) throws IOException {
        this(file.getPath(), file);
    }

    public RandomAccessVolume(String name, File file) throws IOException {
        this(name, file, false);
    }

    public RandomAccessVolume(String name, File file, boolean readOnly) throws IOException {
        this(name, file, readOnly, -1l);
    }

    public RandomAccessVolume(String name, File file, boolean readOnly, long positionLimit) throws IOException {
        this.name = name;
        randomAccessFile = new RandomAccessFile(file, readOnly ? "r" : "rw");
        this.readOnly = readOnly;
        this.positionLimit = positionLimit;
    }

    @Override
    public void close() throws IOException {
        randomAccessFile.close();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long length() {
        try {
            return randomAccessFile.length();
        } catch (IOException e) {
            return -1l;
        }
    }

    @Override
    public DataOutput getDataOutput(long offset) {
        if (readOnly) {
            throw new IllegalArgumentException("Volume is read only");
        }
        // FIXME: Try locking the channel, so that we can force thread safety
        final FileChannel channel = randomAccessFile.getChannel();
        return new RandomAccessDataOutput(channel, offset);
    }

    @Override
    public DataInput getDataInput(long offset) {
        final FileChannel channel = randomAccessFile.getChannel();
        return new RandomAccessDataInput(channel, offset);
    }

    @Override
    public long getPositionLimit() {
        return positionLimit;
    }

    /* FIXME: We need to change this to include length */
    @Override
    public void ensureAvailable(long offset) throws IOException {
    }

    @Override
    public boolean isClosed() {
        return !randomAccessFile.getChannel().isOpen();
    }

    @Override
    public void clear(long startOffset, long endOffset) throws IOException {
        FileChannel channel = randomAccessFile.getChannel();
        FileLock lock = lock(channel, startOffset, endOffset, false);
        try {
            channel.position(startOffset);
            while (startOffset < endOffset) {
                long remaining = Math.min(CLEAR.length, endOffset - startOffset);
                channel.write(ByteBuffer.wrap(Volume.CLEAR, 0, (int) remaining), startOffset);
                startOffset += CLEAR.length;
            }
        } finally {
            unlock(lock);
        }
    }

    private FileLock lock(FileChannel channel, long startOffset, long endOffset, boolean shared) throws IOException {
        if (positionLimit > 0 && endOffset > positionLimit) {
            throw new EOFException("File position limit has been reached");
        }
        FileLock lock = null;
        long time = System.currentTimeMillis();
        while ((lock = (endOffset == -1 ? channel.tryLock() : channel.tryLock(startOffset, endOffset, shared))) == null) {
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

    private class RandomAccessDataInput extends VolumeDataInput {

        private final FileChannel fileChannel;

        public RandomAccessDataInput(FileChannel fileChannel, long position) {
            super(position);
            this.fileChannel = fileChannel;
        }

        @Override
        public byte readByte() throws IOException {
            byte[] b = new byte[1];
            readFully(b, 0, 1);
            return b[0];
        }

        @Override
        public void readFully(byte[] b, int off, int len) throws IOException {
            FileLock lock = null;
            try {
                lock = lock(fileChannel, position.get(), position.get() + (len - off), true);
                ByteBuffer buffer = ByteBuffer.wrap(b, off, len);
                fileChannel.read(buffer, position.getAndAdd(b.length));
            } finally {
                unlock(lock);
            }
        }
    }

    private class RandomAccessDataOutput extends VolumeDataOutput {

        private final FileChannel fileChannel;

        public RandomAccessDataOutput(FileChannel fileChannel, long offset) {
            super(offset);
            this.fileChannel = fileChannel;
        }

        @Override
        public void writeByte(int v) throws IOException {
            write(new byte[] { (byte)v }, 0, 1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            FileLock lock = null;
            try {
                lock = lock(fileChannel, position.get(), position.get() + (len - off), false);
                ByteBuffer buffer = ByteBuffer.wrap(b, off, len);
                fileChannel.write(buffer, position.getAndAdd((len - off)));
            } finally {
                unlock(lock);
            }
        }
    }
}
