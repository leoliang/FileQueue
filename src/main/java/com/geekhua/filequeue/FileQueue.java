package com.geekhua.filequeue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.geekhua.filequeue.exception.FileQueueClosedException;

/**
 * @author Leo Liang
 * 
 */
public interface FileQueue<E> {
    public E get() throws InterruptedException, IOException;

    public E get(long timeout, TimeUnit timeUnit) throws InterruptedException, IOException;

    public void add(E m) throws IOException, FileQueueClosedException;

    public void close();

}
