package com.geekhua.filequeue;

import com.geekhua.filequeue.codec.Codec;
import com.geekhua.filequeue.codec.KryoCodec;

/**
 * 
 * @author Leo Liang
 * @author Macro Huang
 */
public class Config {

	private Codec codec = new KryoCodec();
    private String          name          = "default";
    private String          baseDir       = "/data/appdatas/filequeue";
    private int             msgAvgLen     = 1024;
    private long            readingFileNo = -1L;
    private long            readingOffset = 0L;
    private long            fileSiz       = 1024 * 1024 * 100;
    private boolean         bakReadFile   = false;

    public boolean isBakReadFile() {
        return bakReadFile;
    }

    public void setBakReadFile(boolean bakReadFile) {
        this.bakReadFile = bakReadFile;
    }

    public long getFileSiz() {
        return fileSiz;
    }

    public void setFileSiz(long fileSiz) {
        this.fileSiz = fileSiz;
    }

    public void setCodec(Codec codec) {
        this.codec = codec;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
    }

    public void setMsgAvgLen(int msgAvgLen) {
        this.msgAvgLen = msgAvgLen;
    }

    public void setReadingFileNo(long readingFileNo) {
        this.readingFileNo = readingFileNo;
    }

    public void setReadingOffset(long readingOffset) {
        this.readingOffset = readingOffset;
    }

    public Codec getCodec() {
        return codec;
    }

    public String getName() {
        return name;
    }

    public String getBaseDir() {
        return baseDir;
    }

    public int getMsgAvgLen() {
        return msgAvgLen;
    }

    public long getReadingFileNo() {
        return readingFileNo;
    }

    public long getReadingOffset() {
        return readingOffset;
    }

    public long getFileSize() {
        return fileSiz;
    }

}
