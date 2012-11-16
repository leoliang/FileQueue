package com.geekhua.filequeue.meta;

/**
 * 
 * @author Leo Liang
 * 
 */
public class Meta {
    private long readingFileNo;
    private long readingFileOffset;

    public Meta() {
    }

    public Meta(long readingFileNo, long readingFileOffset) {
        super();
        this.readingFileNo = readingFileNo;
        this.readingFileOffset = readingFileOffset;
    }

    public long getReadingFileNo() {
        return readingFileNo;
    }

    public void setReadingFileNo(long readingFileNo) {
        this.readingFileNo = readingFileNo;
    }

    public long getReadingFileOffset() {
        return readingFileOffset;
    }

    public void setReadingFileOffset(long readingFileOffset) {
        this.readingFileOffset = readingFileOffset;
    }

}
