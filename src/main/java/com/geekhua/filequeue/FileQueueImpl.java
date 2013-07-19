package com.geekhua.filequeue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.geekhua.filequeue.datastore.DataStore;
import com.geekhua.filequeue.datastore.DataStoreImpl;
import com.geekhua.filequeue.exception.FileQueueClosedException;
import com.geekhua.filequeue.meta.MetaHolder;
import com.geekhua.filequeue.meta.MetaHolderImpl;

/**
 * 
 * @author Leo Liang
 * 
 */
public class FileQueueImpl<E> implements FileQueue<E> {
    private DataStore<E>        dataStore;
    private Config              config;
    private MetaHolder          metaHolder;
    private volatile boolean    stopped   = false;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final ReentrantLock readLock  = new ReentrantLock();

    public FileQueueImpl() {
        this(null);
    }

    public FileQueueImpl(Config config) {
        if (config == null) {
            config = new Config();
        }
        try {
            this.metaHolder = new MetaHolderImpl(config.getName(), config.getBaseDir());
            metaHolder.init();
            this.config = config;
            this.config.setReadingFileNo(metaHolder.getReadingFileNo());
            this.config.setReadingOffset(metaHolder.getReadingFileOffset());
            this.dataStore = new DataStoreImpl<E>(this.config);
            dataStore.init();

        } catch (IOException e) {
            throw new RuntimeException("FileQueue init fail.", e);
        }
    }

    public E get() throws InterruptedException, IOException {
        readLock.lockInterruptibly();
        try {
            E res = null;
            while (res == null) {
                res = dataStore.take();
                if (res != null) {
                    metaHolder.update(dataStore.readingFileNo(), dataStore.readingFileOffset());
                    return res;
                }
                // Since res == null not only caused by queue empty,
                // but also last msg not flush to disk completely. We
                // couldn't wait until not empty like
                // LinkedBlockingQueue.poll
                TimeUnit.NANOSECONDS.sleep(1000);
            }
            return null;
        } finally {
            readLock.unlock();
        }
    }

    public E get(long timeout, TimeUnit unit) throws InterruptedException, IOException {
        long startNanos = System.nanoTime();
        long timeoutNanos = unit.toNanos(timeout);
        readLock.lockInterruptibly();
        try {
            E res = null;
            while (res == null) {
                res = dataStore.take();
                if (res != null) {
                    metaHolder.update(dataStore.readingFileNo(), dataStore.readingFileOffset());
                    return res;
                } else {
                    // Since res == null not only caused by queue empty,
                    // but also last msg not flush to disk completely. We
                    // couldn't wait until not empty like
                    // LinkedBlockingQueue.poll
                    if (System.nanoTime() - startNanos < timeoutNanos) {
                        TimeUnit.NANOSECONDS.sleep(100);
                    } else {
                        return null;
                    }
                }
            }
            return null;
        } finally {
            readLock.unlock();
        }
    }

    public void add(E m) throws IOException, FileQueueClosedException {
        writeLock.lock();
        try {
            if (stopped) {
                throw new FileQueueClosedException();
            }
            dataStore.put(m);
        } finally {
            writeLock.unlock();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.geekhua.filequeue.FileQueue#close()
     */
    public void close() {
        writeLock.lock();
        try {
            stopped = true;
            dataStore.close();
        } finally {
            writeLock.unlock();
        }
    }
    
    public long getReadingFileNo(){
        return metaHolder.getReadingFileNo();
    }
    
    public long getReadingFileOffset(){
        return metaHolder.getReadingFileOffset();
    }
    
    public long getWritingFileNo(){
        return dataStore.writingFileNo();
    }
    
    public long getWritingFileOffset(){
        return dataStore.writingFileOffset();
    }

}
