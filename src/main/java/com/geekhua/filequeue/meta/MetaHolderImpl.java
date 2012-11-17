package com.geekhua.filequeue.meta;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.StringUtils;

/**
 * @author Leo Liang
 * 
 */
public class MetaHolderImpl implements MetaHolder {
    private static final String   METAFILE_DIRNAME = "meta";
    private static final String   METAFILE_NAME    = "meta";
    private static final int      METAFILE_SIZE    = 200;
    private static final byte[]   BUF_MASK         = new byte[METAFILE_SIZE];

    private AtomicReference<Meta> meta;
    private File                  baseDir;
    private MappedByteBuffer      mbb;

    public MetaHolderImpl(String name, String baseDir) {
        this.baseDir = new File(new File(baseDir, name), METAFILE_DIRNAME);
    }

    public void update(long readingFileNo, long readingFileOffset) {
        meta.set(new Meta(readingFileNo, readingFileOffset));
        saveToFile(readingFileNo, readingFileOffset);
    }

    public void init() throws IOException {
        createFileIfNeed();
        loadFromFile();
    }

    private void createFileIfNeed() throws IOException {
        if (!baseDir.exists()) {
            baseDir.mkdirs();
        }

        File f = getMetaFile();
        if (!f.exists()) {
            f.createNewFile();
        }
    }

    private File getMetaFile() {
        return new File(baseDir, METAFILE_NAME);
    }

    private synchronized void saveToFile(long readingFileNo, long readingFileOffset) {
        mbb.position(0);
        mbb.put(BUF_MASK);
        mbb.position(0);
        mbb.put(String.valueOf(readingFileNo).getBytes());
        mbb.put("\n".getBytes());
        mbb.put(String.valueOf(readingFileOffset).getBytes());
        mbb.put("\n".getBytes());
    }

    private void loadFromFile() throws IOException {
        File f = getMetaFile();

        FileReader fr = null;
        BufferedReader br = null;

        try {
            fr = new FileReader(f);
            br = new BufferedReader(fr);
            String readingFileNoStr = br.readLine();
            String readingFileOffsetStr = br.readLine();
            this.meta = new AtomicReference<Meta>(new Meta(
                    StringUtils.isNumeric(readingFileNoStr) ? Long.valueOf(readingFileNoStr) : -1L,
                    StringUtils.isNumeric(readingFileOffsetStr) ? Long.valueOf(readingFileOffsetStr) : 0L));
            this.mbb = new RandomAccessFile(f, "rwd").getChannel().map(MapMode.READ_WRITE, 0, METAFILE_SIZE);

        } finally {
            if (fr != null) {
                try {
                    fr.close();
                } catch (IOException e) {
                    // TODO
                }
            }
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    // TODO
                }
            }
        }

    }

    public long getReadingFileNo() {
        return meta.get().getReadingFileNo();
    }

    public long getReadingFileOffset() {
        return meta.get().getReadingFileOffset();
    }

}
