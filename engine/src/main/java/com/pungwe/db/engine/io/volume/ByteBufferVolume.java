package com.pungwe.db.engine.io.volume;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Arrays;

// FIXME: This could form the basis for all the volumes
/**
 * Created by 917903 on 23/05/2016.
 */
public abstract class ByteBufferVolume extends AbstractGrowableVolume {

    protected static final int VOLUME_PAGE_SHIFT = 20; // 1 MB

    protected final String name;
    protected final int sliceShift;
    protected final int sliceSizeModMask;
    protected final int sliceSize;
    protected final long positionLimit;

    protected volatile ByteBuffer[] slices = new ByteBuffer[0];

    protected final boolean readOnly;

    public ByteBufferVolume(String name, boolean readOnly, int sliceShift, long positionLimit) {
        this.name = name;
        this.readOnly = readOnly;
//        this.sliceShift = (sliceShift < Constants.MIN_PAGE_SHIFT ? Constants.MIN_PAGE_SHIFT : sliceShift);
        this.sliceShift = sliceShift;
        this.sliceSize = 1 << this.sliceShift; // MAX_SIZE is 2GB
        this.sliceSizeModMask = sliceSize - 1;
        this.positionLimit = positionLimit;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public long getPositionLimit() {
        return positionLimit;
    }

    public abstract ByteBuffer makeNewBuffer(long offset) throws IOException;

    @Override
    public void clear(long startOffset, long endOffset) throws IOException {
        ByteBuffer buf = slices[(int) (startOffset >>> sliceShift)];
        int start = (int) (startOffset & sliceSizeModMask);
        int end = (int) (endOffset & sliceSizeModMask);

        int pos = start;
        while (pos < end) {
            buf = buf.duplicate();
            buf.position(pos);
            buf.put(CLEAR, 0, Math.min(CLEAR.length, end - pos));
            pos += CLEAR.length;
        }
    }

    // Copied from MapDB
    @Override
    public void ensureAvailable(long offset) throws IOException {
        int slicePos = (int) (offset >>> sliceShift);

        // check for most common case, this is already mapped
        if (slicePos < slices.length) {
            return;
        }
        lock();
        try {
            // Check a second time
            if (slicePos < slices.length) {
                return;
            }

            int oldSize = slices.length;
            ByteBuffer[] slices2 = slices;

            slices2 = Arrays.copyOf(slices2, Math.max(slicePos + 1, slices2.length + slices2.length / 1000));

            for (int pos = oldSize; pos < slices2.length; pos++) {
                slices2[pos] = makeNewBuffer(1L * sliceSize * pos);
            }

            slices = slices2;
        } finally {
            unlock();
        }
    }

    @Override
    public long length() throws IOException {
        return ((long) slices.length) * sliceSize;
    }

    @Override
    public DataInput getDataInput(long offset) {
        return new ByteBufferVolumeDataInput(offset);
    }

    @Override
    public DataOutput getDataOutput(long offset) {
        return new ByteBufferVolumeDataOutput(offset);
    }

    /**
     * Hack to unmap MappedByteBuffer.
     * Unmap is necessary on Windows, otherwise file is locked until JVM exits or BB is GCed.
     * There is no public JVM API to unmap buffer, so this tries to use SUN proprietary API for unmap.
     * Any error is silently ignored (for example SUN API does not exist on Android).
     */
    protected void unmap(MappedByteBuffer b) {
        try {
            if (unmapHackSupported) {

                // need to dispose old direct buffer, see bug
                // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038
                Method cleanerMethod = b.getClass().getMethod("cleaner", new Class[0]);
                if (cleanerMethod != null) {
                    cleanerMethod.setAccessible(true);
                    Object cleaner = cleanerMethod.invoke(b);
                    if (cleaner != null) {
                        Method clearMethod = cleaner.getClass().getMethod("clean", new Class[0]);
                        if (clearMethod != null)
                            clearMethod.invoke(cleaner);
                    }
                }
            }
        } catch (Exception e) {
            unmapHackSupported = false;
            //TODO exception handling
            //Utils.LOG.log(Level.WARNING, "ByteBufferVol Unmap failed", e);
        }
    }

    private static boolean unmapHackSupported = true;

    static {
        try {
            final ClassLoader loader = Thread.currentThread().getContextClassLoader();
            unmapHackSupported = Class.forName("sun.nio.ch.DirectBuffer", true, loader) != null;
        } catch (Exception e) {
            unmapHackSupported = false;
        }
    }

    // Workaround for File locking after .close() on Windows.
    private static boolean windowsWorkaround = System.getProperty("os.name").toLowerCase().startsWith("win");

    private final class ByteBufferVolumeDataInput extends VolumeDataInput {

        public ByteBufferVolumeDataInput(long offset) {
            super(offset);
        }

        @Override
        public void readFully(byte[] b, int off, int len) throws IOException {
            long endOffset = position.longValue() + (len - off);
            if (endOffset > length()) {
                throw new EOFException("Reached the end of the volume");
            }
            int startPos = (int)(position.get() >>> sliceShift);
            int endPos = (int)(endOffset >>> sliceShift);
            int remaining = len - off;
            for (int i = startPos; i < endPos + 1; i++) {
                if (remaining <= 0) {
                    break;
                }
                int s = (int)position.get() & sliceSizeModMask;
                int n = Math.min((sliceSize - s), remaining);
                ByteBuffer in = slices[i];
                in.position(s);
                for (int j = off; j < (off + n); j++) {
                    b[j] = in.get();
                }
                off += n;
                remaining -= n;
                position.addAndGet(n);
            }
        }
    }

    private final class ByteBufferVolumeDataOutput extends VolumeDataOutput {

        public ByteBufferVolumeDataOutput(long offset) {
            super(offset);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            long endOffset = position.longValue() + (len - off);
            ensureAvailable(endOffset); // Ensure we have enough space
            int startPos = (int)(position.get() >>> sliceShift);
            int endPos = (int)(endOffset >>> sliceShift);
            // start position
            int remaining = len - off;
            for (int i = startPos; i < endPos + 1; i++) {
                if (remaining <= 0) {
                    break;
                }
                int s = (int)position.get() & sliceSizeModMask;
                ByteBuffer out = slices[i];
                out.position(s);
                int n = Math.min((sliceSize - s), remaining);
                for (int j = off; j < (off + n); j++) {
                    out.put(b[j]);
                }
                off += n;
                remaining -= n;
                position.addAndGet(n);
            }
        }
    }
}
