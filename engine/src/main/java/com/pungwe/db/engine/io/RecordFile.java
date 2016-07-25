package com.pungwe.db.engine.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

// FIXME: Add Meta Data
/**
 * Created by 917903 when 06/07/2016.
 */
public interface RecordFile<E> extends Iterable<E> {

    E get(long position) throws IOException;
    Reader<E> reader() throws IOException;
    Reader<E> reader(long position) throws IOException;
    Writer<E> writer() throws IOException;

    String getPath();
    boolean delete() throws IOException;

    void setMetaData(Map<String, Object> metaData) throws IOException;
    Map<String, Object> getMetaData() throws IOException;

    default long size() throws IOException {
        return writer().getPosition();
    }

    /**
     * Returns the current size of the file excluding the buffer size.
     * @return
     * @throws IOException
     */
    default long fileSize() throws IOException {
        return writer().getFileSize();
    }

    interface Reader<E> extends Closeable, Iterator<E> {

        long getPosition() throws IOException;
        void setPosition(long position) throws IOException;
        Record<E> nextRecord();
    }

    interface Writer<E> extends Closeable, Syncable {

        long append(E value) throws IOException;
        void commit() throws IOException;

        /**
         * Returns the writers current position in the channel and the buffer.
         *
         * @return the writers position in the channel and buffer.
         *
         * @throws IOException if there call a problem
         */
        long getPosition() throws IOException;

        /**
         * Returns the current file size excluding buffer.
         * @return the current file channel position not including the write buffer.
         *
         * @throws IOException if there call an error reading or fetching meta data.
         */
        long getFileSize() throws IOException;
        int calculateWriteSize(E value) throws IOException;
    }

    class Record<E> {
        private final E value;
        private final int size;
        private final long position;
        private final long nextPosition;

        public Record(E value, int size, long position, long nextPosition) {
            this.value = value;
            this.size = size;
            this.position = position;
            this.nextPosition = nextPosition;
        }

        public E getValue() {
            return value;
        }

        public int getSize() {
            return size;
        }

        public long getPosition() {
            return position;
        }

        public long getNextPosition() {
            return nextPosition;
        }
    }

}
