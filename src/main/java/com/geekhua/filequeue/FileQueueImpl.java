package com.geekhua.filequeue;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
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
    private DataStore<E>                        dataStore;
    private Config                              config;
    private MetaHolder                          metaHolder;
    private BlockingQueue<PrefetchCacheItem<E>> prefetchCache;
    private volatile boolean                    stopped   = false;
    private final ReentrantLock                 writeLock = new ReentrantLock();
    private final ReentrantLock                 readLock  = new ReentrantLock();

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
            prefetchCache = new LinkedBlockingQueue<PrefetchCacheItem<E>>(config.getCacheSize());

            Thread prefetchThread = new Thread(new Runnable() {

                public void run() {
                    while (!stopped) {
                        try {
                            E e = dataStore.take();
                            PrefetchCacheItem<E> cacheItem = new PrefetchCacheItem<E>(e, dataStore.readingFileOffset(),
                                    dataStore.readingFileNo());
                            if (e != null) {
                                prefetchCache.put(cacheItem);
                            } else {
                                Thread.sleep(10);
                            }
                        } catch (Exception e) {
                            // TODO
                        }
                    }
                }
            });
            prefetchThread.setName("FileQueue-" + config.getName() + "-prefetchThread");
            prefetchThread.setDaemon(true);
            prefetchThread.start();
        } catch (IOException e) {
            throw new RuntimeException("FileQueue init fail.", e);
        }
    }

    public E get() throws InterruptedException {
        readLock.lock();
        try {
            PrefetchCacheItem<E> res = prefetchCache.take();
            metaHolder.update(res.getReadingFileNo(), res.getReadingOffset());
            return res.getElement();
        } finally {
            readLock.unlock();
        }
    }

    public E get(long timeout, TimeUnit timeUnit) throws InterruptedException {
        readLock.lock();
        try {
            PrefetchCacheItem<E> res = prefetchCache.poll(timeout, timeUnit);
            if (res != null) {
                metaHolder.update(res.getReadingFileNo(), res.getReadingOffset());
            }
            return res.getElement();
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

    private static class PrefetchCacheItem<E> {
        private E    element;
        private long readingOffset;
        private long readingFileNo;

        public PrefetchCacheItem(E element, long readingOffset, long readingFileNo) {
            super();
            this.element = element;
            this.readingOffset = readingOffset;
            this.readingFileNo = readingFileNo;
        }

        public E getElement() {
            return element;
        }

        public long getReadingOffset() {
            return readingOffset;
        }

        public long getReadingFileNo() {
            return readingFileNo;
        }

    }

}
