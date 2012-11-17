package com.geekhua.filequeue.datastore;

import java.io.IOException;

/**
 * 
 * @author Leo Liang
 * 
 */
public interface DataStore<E> {

    void put(E element) throws IOException;

    E take() throws IOException;

    void init() throws IOException;

    void close();

    long readingFileOffset();

    long readingFileNo();
}
