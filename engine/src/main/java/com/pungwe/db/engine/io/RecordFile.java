package com.pungwe.db.engine.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by 917903 on 06/07/2016.
 */
public interface RecordFile<E> extends Iterable<E> {

    E get(long position);
    Reader<E> reader() throws IOException;
    Reader<E> reader(long position) throws IOException;

    public interface Reader<E> extends Closeable, Iterator<E> {

        long getPosition() throws IOException;
        void setPosition(long position) throws IOException;

    }

    public interface Writer<E> extends Closeable, Syncable {

        long append(E value) throws IOException;

    }

}
