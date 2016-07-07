package com.pungwe.db.engine.io.volume;

import com.pungwe.db.engine.io.AbstractDataInput;
import com.pungwe.db.engine.io.AbstractDataOutput;
import com.pungwe.db.engine.utils.Constants;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ian on 15/06/2016.
 */
public final class SegmentedFileVolume extends AbstractGrowableVolume {

    protected final String directory;
    protected final String name;
    protected final String suffix;
    protected final static int sliceShift = 30;
    protected final static int sliceSize = 2 << sliceShift;
    protected final static int sliceSizeModMask = sliceSize - 1;
    protected final AtomicInteger nextSegment;
    protected final boolean memoryMapped;
    protected final boolean readOnly;
    protected Volume[] volumes = new Volume[0];

    public SegmentedFileVolume(String directory, String name, String suffix) throws IOException {
        // Default to writable direct file, no memory mapping
        this(directory, name, suffix, false, false);
    }

    public SegmentedFileVolume(String directory, String name, String suffix, boolean readOnly) throws IOException {
        // Default the direct file with no memory mapping
        this(directory, name, suffix, readOnly, false);
    }

    public SegmentedFileVolume(String directory, String name, String suffix, boolean readOnly, boolean memoryMapped)
            throws IOException {
        this.directory = directory;
        this.name = name;
        this.suffix = suffix;
        this.nextSegment = new AtomicInteger();
        this.readOnly = readOnly;
        this.memoryMapped = memoryMapped;
        // Find all the segments...
        findSegments();
    }

    protected void findSegments() throws IOException {
        File directory = new File(this.directory);
        String[] files = directory.list((dir, fileName) -> fileName.startsWith(name));
        for (String file : files) {
            int segmentNumber = -1;
            Pattern p = Pattern.compile("^" + name + "\\.(\\d+)\\." + suffix + "$");
            Matcher m = p.matcher(file);
            if (m.matches() && m.groupCount() > 0) {
                segmentNumber = new Integer(m.group(1));
            } else {
                continue;
            }
            Volume segment;
            if (memoryMapped) {
                segment = new MemoryMappedVolume(file, new File(directory, file), readOnly, Constants.MIN_PAGE_SHIFT,
                        sliceSize);
            } else {
                segment = new RandomAccessFileVolume(file, new File(directory, file), readOnly, sliceSize);
            }
            if (volumes.length < segmentNumber) {
                volumes = Arrays.copyOf(volumes, segmentNumber);
            }
            volumes[segmentNumber] = segment;
            if (nextSegment.longValue() <= segmentNumber) {
                nextSegment.set(segmentNumber + 1);
            }
        }
    }

    protected final void makeNewSegment() throws IOException {
        int s = nextSegment.getAndIncrement();
        String segmentName = this.name + "." + s + "." + suffix;
        Volume segment;
        if (memoryMapped) {
            segment = new MemoryMappedVolume(segmentName, new File(segmentName), readOnly, Constants.MIN_PAGE_SHIFT,
                    sliceSize);
        } else {
            segment = new RandomAccessFileVolume(segmentName, new File(segmentName), readOnly, sliceSize);
        }
        volumes = Arrays.copyOf(volumes, volumes.length + 1);
        volumes[s] = segment;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long length() throws IOException {
        return sliceSize * volumes.length;
    }

    @Override
    public DataOutput getDataOutput(long offset) {
        return new SegmentedFileDataOutput(offset);
    }

    @Override
    public DataInput getDataInput(long offset) {
        return new SegmentedFileDataInput(offset);
    }

    @Override
    public long getPositionLimit() {
        return -1;
    }

    @Override
    public void ensureAvailable(long offset) throws IOException {
        lock();
        try {
            int slicePos = (int) (offset >>> sliceShift);
            // check for most common case, that this is already mapped
            if (slicePos <= volumes.length) {
                return;
            }
            int oldSize = volumes.length;
            for (int i = oldSize; i < slicePos + 1; i++) {
                makeNewSegment();
            }
        } finally {
            unlock();
        }
    }

    @Override
    public boolean isClosed() {
        return volumes.length == 0;
    }

    @Override
    public void clear(long startOffset, long endOffset) throws IOException {
        // FIXME: We should put a proper segment cleaner in place here.
    }

    @Override
    public void close() throws IOException {
        for (int i = 0; i < volumes.length; i++) {
            Volume v = volumes[i];
            if (v == null) {
                continue;
            }
            v.close();
        }
        volumes = new Volume[0];
    }

    private class SegmentedFileDataInput extends AbstractDataInput {

        public SegmentedFileDataInput(long position) {
            super(position);
        }

        @Override
        public byte readByte() throws IOException {
            byte[] b = new byte[1];
            readFully(b, 0 ,1);
            return b[0];
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
                int n = (int)Math.min((sliceSize - s), remaining);
                Volume volume = volumes[i];
                DataInput in = volume.getDataInput(s);
                in.readFully(b, off, n);
                off += n;
                remaining -= n;
            }
        }
    }

    private class SegmentedFileDataOutput extends AbstractDataOutput {

        public SegmentedFileDataOutput(long position) {
            super(position);
        }

        @Override
        public void writeByte(int v) throws IOException {
            byte[] b = new byte[] { (byte)v };
            write(b, 0 ,1);
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
                Volume volume = volumes[i];
                int n = (int)Math.min((sliceSize - s), remaining);
                DataOutput out = volume.getDataOutput(s);
                out.write(b, off, n);
                off += n;
                remaining -= n;
                position.addAndGet(n);
            }
        }
    }
}
