package com.geekhua.filequeue.meta;

import java.io.IOException;

/**
 * @author Leo Liang
 * 
 */
public interface MetaHolder {
    void update(long readingFileNo, long readingFileOffset);

    void init() throws IOException;

    long getReadingFileNo();

    long getReadingFileOffset();
}
