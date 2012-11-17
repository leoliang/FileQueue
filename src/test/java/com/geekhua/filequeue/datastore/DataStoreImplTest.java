package com.geekhua.filequeue.datastore;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.geekhua.filequeue.Config;
import com.geekhua.filequeue.Config.CodecType;
import com.geekhua.filequeue.codec.Codec;
import com.geekhua.filequeue.codec.CodecFactory;

/**
 * @author Leo Liang
 * 
 */
public class DataStoreImplTest {
    private static final File   baseDir              = new File(System.getProperty("java.io.tmpdir", "."),
                                                             "datastoreTest");
    private static final byte[] DATAFILE_END_CONTENT = new byte[] { (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
            (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAB };
    private static final byte[] HEADER               = new byte[] { (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAB };
    private static final int    CHECKSUMLEN          = 20;

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
    @SuppressWarnings("unchecked")
    public void testPutFileSwap() throws Exception {
        Config config = new Config();
        config.setMsgAvgLen(10);
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setFileSiz(10);
        DataStore<String> ds = new DataStoreImpl<String>(config);
        ds.init();
        String content = "0123456789";
        for (int i = 0; i < 2; i++) {
            ds.put(content);
        }
        Collection<File> listFiles = (Collection<File>) FileUtils.listFiles(baseDir, new String[] { "fq" }, true);

        Assert.assertEquals(2, listFiles.size());

        int i = 0;

        for (File file : listFiles) {
            RandomAccessFile fis = new RandomAccessFile(file, "r");
            BlockGroup blockGroup = BlockGroup.read(fis, BlockGroup.estimateBlockSize(10));
            Codec<String> codec = CodecFactory.getInstance(CodecType.JAVAOBJECT);
            Assert.assertEquals(content, codec.decode(blockGroup.getContent()));

            if (i == 0) {
                blockGroup = BlockGroup.read(fis, BlockGroup.estimateBlockSize(10));
                Assert.assertArrayEquals(getEndBlockGroup().getContent(), blockGroup.getContent());
                fis.close();
            }
            i++;
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPutCorruptFile() throws Exception {
        Config config = new Config();
        config.setMsgAvgLen(10);
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setFileSiz(40);
        DataStore<byte[]> ds = new DataStoreImpl<byte[]>(config);
        ds.init();
        byte[] content = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        Collection<File> listFiles = (Collection<File>) FileUtils.listFiles(baseDir, new String[] { "fq" }, true);
        if (listFiles.isEmpty() || listFiles.size() != 1) {
            Assert.fail();
        } else {
            RandomAccessFile file = new RandomAccessFile(listFiles.iterator().next(), "rw");
            file.write(new byte[] { 1, 2, 3 });
            ds.put(content);
            Codec<byte[]> codec = CodecFactory.getInstance(CodecType.JAVAOBJECT);
            int contentLen = codec.encode(content).length + HEADER.length + CHECKSUMLEN + 4;
            Assert.assertEquals(
                    BlockGroup.estimateBlockSize(10)
                            + (contentLen / BlockGroup.estimateBlockSize(10) + (contentLen
                                    % BlockGroup.estimateBlockSize(10) == 0 ? 0 : 1))
                            * BlockGroup.estimateBlockSize(10), file.length());
            file.seek(0);
            BlockGroup blockGroup = BlockGroup.read(file, BlockGroup.estimateBlockSize(10));
            Assert.assertArrayEquals(content, codec.decode(blockGroup.getContent()));
            file.close();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testLastDataFileRecover() throws Exception {
        Config config = new Config();
        config.setMsgAvgLen(10);
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setFileSiz(40);
        DataStore<byte[]> ds = new DataStoreImpl<byte[]>(config);
        ds.init();
        Collection<File> listFiles = (Collection<File>) FileUtils.listFiles(baseDir, new String[] { "fq" }, true);
        if (listFiles.isEmpty() || listFiles.size() != 1) {
            Assert.fail();
        } else {
            RandomAccessFile file = new RandomAccessFile(listFiles.iterator().next(), "rw");
            file.write(new byte[] { 1, 2, 3 });
            ds = new DataStoreImpl<byte[]>(config);
            ds.init();

            file.seek(0);

            BlockGroup blockGroup = BlockGroup.read(file, BlockGroup.estimateBlockSize(10));
            Assert.assertArrayEquals(getEndBlockGroup().getContent(), blockGroup.getContent());
            file.close();
        }

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testLastDataFileRecover2() throws Exception {
        Config config = new Config();
        config.setMsgAvgLen(10);
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setFileSiz(40);
        DataStore<byte[]> ds = new DataStoreImpl<byte[]>(config);
        ds.init();
        byte[] content = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        ds.put(content);
        Collection<File> listFiles = (Collection<File>) FileUtils.listFiles(baseDir, new String[] { "fq" }, true);
        if (listFiles.isEmpty() || listFiles.size() != 1) {
            Assert.fail();
        } else {
            RandomAccessFile file = new RandomAccessFile(listFiles.iterator().next(), "rw");
            file.seek(file.length());
            file.write(new byte[] { 1, 2, 3 });
            ds = new DataStoreImpl<byte[]>(config);
            ds.init();

            file.seek(0);

            Codec<byte[]> codec = CodecFactory.getInstance(CodecType.JAVAOBJECT);
            BlockGroup blockGroup = BlockGroup.read(file, BlockGroup.estimateBlockSize(10));
            Assert.assertArrayEquals(content, codec.decode(blockGroup.getContent()));
            blockGroup = BlockGroup.read(file, BlockGroup.estimateBlockSize(10));
            Assert.assertArrayEquals(getEndBlockGroup().getContent(), blockGroup.getContent());
            file.close();
        }
    }

    @Test
    public void testTake() throws Exception {
        Config config = new Config();
        config.setMsgAvgLen(10);
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setFileSiz(20);
        DataStore<Integer> ds = new DataStoreImpl<Integer>(config);
        ds.init();
        int times = 10;
        for (int i = 0; i < times; i++) {
            ds.put(i);
        }

        for (int i = 0; i < times; i++) {
            Assert.assertEquals(Integer.valueOf(i), ds.take());
        }
    }

    @Test
    public void testTakeWhileWriting() throws Exception {
        Config config = new Config();
        config.setMsgAvgLen(10);
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setFileSiz(20);
        DataStore<Integer> ds = new DataStoreImpl<Integer>(config);
        ds.init();
        int times = 10;
        for (int i = 0; i < times; i++) {
            ds.put(i);
            Assert.assertEquals(Integer.valueOf(i), ds.take());
            Assert.assertNull(ds.take());
        }

    }

    @Test
    public void testTake2() throws Exception {
        Config config = new Config();
        config.setMsgAvgLen(100);
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setFileSiz(1024);
        DataStore<Integer> ds = new DataStoreImpl<Integer>(config);
        ds.init();
        int times = 1000;
        for (int i = 0; i < times; i++) {
            ds.put(i);
        }

        for (int i = 0; i < times; i++) {
            Assert.assertEquals(Integer.valueOf(i), ds.take());
        }
    }

    @Test
    public void testTakeWhileWriting2() throws Exception {
        Config config = new Config();
        config.setMsgAvgLen(100);
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setFileSiz(1024);
        DataStore<Integer> ds = new DataStoreImpl<Integer>(config);
        ds.init();
        int times = 1000;
        for (int i = 0; i < times; i++) {
            ds.put(i);
            Assert.assertEquals(Integer.valueOf(i), ds.take());
            Assert.assertNull(ds.take());
        }

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDeleteReadFile() throws Exception {
        Config config = new Config();
        config.setMsgAvgLen(10);
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setFileSiz(38);
        DataStore<byte[]> ds = new DataStoreImpl<byte[]>(config);
        ds.init();
        byte[] content = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        int times = 100;
        for (int i = 0; i < times; i++) {
            ds.put(content);
        }

        Collection<File> listFiles = (Collection<File>) FileUtils.listFiles(baseDir, new String[] { "fq" }, true);
        Assert.assertEquals(times, listFiles.size());

        for (int i = 0; i < times; i++) {
            ds.take();
            ds.clearExpireDataFiles(ds.readingFileNo());
            listFiles = (Collection<File>) FileUtils.listFiles(baseDir, new String[] { "fq" }, true);
            Assert.assertEquals(times - i, listFiles.size());
        }

        listFiles = (Collection<File>) FileUtils.listFiles(baseDir, new String[] { "fq" }, true);
        Assert.assertEquals(1, listFiles.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBakReadFile() throws Exception {
        Config config = new Config();
        config.setMsgAvgLen(10);
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setFileSiz(38);
        config.setBakReadFile(true);
        DataStore<byte[]> ds = new DataStoreImpl<byte[]>(config);
        ds.init();
        byte[] content = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        int times = 100;
        for (int i = 0; i < times; i++) {
            ds.put(content);
        }

        Collection<File> dataFiles = (Collection<File>) FileUtils.listFiles(new File(baseDir, "default/data"),
                new String[] { "fq" }, true);
        Collection<File> bakFiles = (Collection<File>) FileUtils.listFiles(new File(baseDir, "default/bak"),
                new String[] { "fq" }, true);
        Assert.assertEquals(times, dataFiles.size());
        Assert.assertTrue(bakFiles.isEmpty());

        for (int i = 0; i < times; i++) {
            ds.take();
            ds.clearExpireDataFiles(ds.readingFileNo());
            dataFiles = (Collection<File>) FileUtils.listFiles(new File(baseDir, "default/data"),
                    new String[] { "fq" }, true);
            bakFiles = (Collection<File>) FileUtils.listFiles(new File(baseDir, "default/bak"), new String[] { "fq" },
                    true);
            Assert.assertEquals(times - i, dataFiles.size());
            Assert.assertEquals(i, bakFiles.size());
        }

        dataFiles = (Collection<File>) FileUtils.listFiles(new File(baseDir, "default/data"), new String[] { "fq" },
                true);
        bakFiles = (Collection<File>) FileUtils
                .listFiles(new File(baseDir, "default/bak"), new String[] { "fq" }, true);
        Assert.assertEquals(1, dataFiles.size());
        Assert.assertEquals(times - 1, bakFiles.size());
    }

    @Test
    public void testWriteSpeed() throws Exception {
        Config config = new Config();
        config.setMsgAvgLen(1024);
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setFileSiz(1024 * 1024 * 500);
        DataStore<byte[]> ds = new DataStoreImpl<byte[]>(config);
        byte[] content = new byte[1024];
        for (int i = 0; i < 1024; i++) {
            content[i] = 0x55;
        }
        ds.init();
        int times = 100000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
            ds.put(content);
        }
        System.out.println("[Write]Time spend " + (System.currentTimeMillis() - start) + "ms for " + times
                + " times. Avg msg length 1024bytes, each data file 500MB.");

    }

    @Test
    public void testReadSpeed() throws Exception {
        Config config = new Config();
        config.setMsgAvgLen(1024);
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setFileSiz(1024 * 1024 * 500);
        DataStore<byte[]> ds = new DataStoreImpl<byte[]>(config);
        byte[] content = new byte[1024];
        for (int i = 0; i < 1024; i++) {
            content[i] = 0x55;
        }
        ds.init();
        int times = 100000;

        for (int i = 0; i < times; i++) {
            ds.put(content);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
            ds.take();
        }
        System.out.println("[Read]Time spend " + (System.currentTimeMillis() - start) + "ms for " + times
                + " times. Avg msg length 1024bytes, each data file 500MB.");

    }

    private BlockGroup getEndBlockGroup() {
        BlockGroup endBlockGroup = BlockGroup.allocate(DATAFILE_END_CONTENT.length, 10);
        endBlockGroup.setContent(DATAFILE_END_CONTENT);
        return endBlockGroup;
    }
}
