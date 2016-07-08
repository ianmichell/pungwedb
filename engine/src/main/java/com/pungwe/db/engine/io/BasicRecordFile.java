package com.pungwe.db.engine.io;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by ian on 07/07/2016.
 */
public class BasicRecordFile<E> implements RecordFile<E> {

    @Override
    public E get(long position) {
        return null;
    }

    @Override
    public Reader<E> reader() throws IOException {
        return null;
    }

    @Override
    public Reader<E> reader(long position) throws IOException {
        return null;
    }

    @Override
    public Writer<E> writer() throws IOException {
        return null;
    }

    @Override
    public Iterator<E> iterator() {
        return null;
    }

    private class BasicRecordFileReader implements Reader<E> {

        @Override
        public long getPosition() throws IOException {
            return 0;
        }

        @Override
        public void setPosition(long position) throws IOException {

        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public E next() {
            return null;
        }
    }

    private class BasicRecordFileWriter implements Writer<E> {

        @Override
        public long append(E value) throws IOException {
            return 0;
        }

        @Override
        public void sync() throws IOException {

        }

        @Override
        public void close() throws IOException {

        }
    }
}
