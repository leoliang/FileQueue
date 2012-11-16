package com.geekhua.filequeue.meta;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Leo Liang
 * 
 */
public class MetaHolderImplTest {
    private static final File   baseDir       = new File(System.getProperty("java.io.tmpdir", "."), "metaHolderTest");
    private static final int    METAFILE_SIZE = 200;
    private static final byte[] BUF_MASK      = new byte[METAFILE_SIZE];

    @Before
    public void before() throws Exception {
        if (baseDir.exists()) {
            FileUtils.deleteDirectory(baseDir);
        }
        baseDir.mkdirs();
    }

    @After
    public void after() throws Exception {
        if (baseDir.exists()) {
            FileUtils.deleteDirectory(baseDir);
        }
    }

    @Test
    public void testInitWithoutFile() throws Exception {
        MetaHolder holder = new MetaHolderImpl("test", baseDir.getAbsolutePath());
        holder.init();
        File metaFile = new File(baseDir, "test/meta");
        Assert.assertTrue(metaFile.exists());
        Assert.assertEquals(-1L, holder.getReadingFileNo());
        Assert.assertEquals(0L, holder.getReadingFileOffset());
    }

    @Test
    public void testInitWithFile() throws Exception {
        MetaHolder holder = new MetaHolderImpl("test", baseDir.getAbsolutePath());
        File metaFile = new File(baseDir, "test/meta");
        metaFile.mkdirs();
        RandomAccessFile metaFileRac = new RandomAccessFile(new File(baseDir, "test/meta/meta"), "rwd");
        MappedByteBuffer map = metaFileRac.getChannel().map(MapMode.READ_WRITE, 0, METAFILE_SIZE);
        map.position(0);
        map.put(BUF_MASK);
        map.position(0);
        map.put("1111".getBytes());
        map.put("\n".getBytes());
        map.put("2222".getBytes());
        map.put("\n".getBytes());
        metaFileRac.close();
        holder.init();
        Assert.assertEquals(1111L, holder.getReadingFileNo());
        Assert.assertEquals(2222L, holder.getReadingFileOffset());
    }

    @Test
    public void testUpdate() throws Exception {
        MetaHolder holder = new MetaHolderImpl("test", baseDir.getAbsolutePath());
        File metaFile = new File(baseDir, "test/meta");
        metaFile.mkdirs();
        RandomAccessFile metaFileRac = new RandomAccessFile(new File(baseDir, "test/meta/meta"), "rwd");
        MappedByteBuffer map = metaFileRac.getChannel().map(MapMode.READ_WRITE, 0, METAFILE_SIZE);
        map.position(0);
        map.put(BUF_MASK);
        map.position(0);
        map.put("1111".getBytes());
        map.put("\n".getBytes());
        map.put("2222".getBytes());
        map.put("\n".getBytes());
        metaFileRac.close();
        holder.init();
        holder.update(333, 444);
        Assert.assertEquals(333L, holder.getReadingFileNo());
        Assert.assertEquals(444L, holder.getReadingFileOffset());

        holder = new MetaHolderImpl("test", baseDir.getAbsolutePath());
        holder.init();
        Assert.assertEquals(333L, holder.getReadingFileNo());
        Assert.assertEquals(444L, holder.getReadingFileOffset());
    }
}
