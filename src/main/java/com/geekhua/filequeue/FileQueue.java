package com.geekhua.filequeue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author Leo Liang
 * 
 */
public interface FileQueue<E> {
    public E get() throws InterruptedException;

    public E get(long timeout, TimeUnit timeUnit) throws InterruptedException;

    public void add(E m) throws IOException;

    public void close();

}
