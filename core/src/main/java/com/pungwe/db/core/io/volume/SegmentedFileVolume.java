package com.pungwe.db.core.io.volume;

import com.pungwe.db.core.utils.Constants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ian on 15/06/2016.
 */
public final class SegmentedFileVolume implements Volume {

    protected final String name;
    protected final String suffix;
    protected final static int sliceShift = 30;
    protected final static int sliceSize = 2 << sliceShift;
    protected final static int sliceSizeModMask = sliceSize - 1;
    protected final AtomicInteger nextSegment;
    protected final boolean memoryMapped;
    protected final boolean readOnly;
    protected Volume[] volumes = new Volume[0];

    public SegmentedFileVolume(String name, String suffix) throws IOException {
        // Default to direct file, no memory mapping
        this(name, suffix, false, false);
    }

    public SegmentedFileVolume(String name, String suffix, boolean readOnly) throws IOException {
        this(name, suffix, readOnly, false);
    }

    public SegmentedFileVolume(String name, String suffix, boolean readOnly, boolean memoryMapped)
            throws IOException {
        this.name = name;
        this.suffix = suffix;
        this.nextSegment = new AtomicInteger();
        this.readOnly = readOnly;
        this.memoryMapped = memoryMapped;
        // Look at the volume meta data...
    }

    protected final void makeNewSegment() throws IOException {
        int s = nextSegment.getAndIncrement();
        String segmentName = this.name + "." + s + "." + suffix;
        Volume segment;
        if (memoryMapped) {
            segment = new MemoryMappedVolume(segmentName, new File(segmentName), readOnly, Constants.MIN_PAGE_SHIFT,
                    sliceSize);
        } else {
            segment = new RandomAccessVolume(segmentName, new File(segmentName), readOnly, sliceSize);
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
        return 0;
    }

    @Override
    public DataOutput getDataOutput(long offset) {
        return null;
    }

    @Override
    public DataInput getDataInput(long offset) {
        return null;
    }

    @Override
    public long getPositionLimit() {
        return -1l;
    }

    @Override
    public void ensureAvailable(long offset) throws IOException {

    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void clear(long startOffset, long endOffset) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
