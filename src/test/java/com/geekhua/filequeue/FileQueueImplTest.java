package com.geekhua.filequeue;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Leo Liang
 * 
 */
public class FileQueueImplTest {
    private static final File baseDir = new File(System.getProperty("java.io.tmpdir", "."), "fileQueueImplTest");

    // private static final File baseDir = new
    // File("/Volumes/HDD/data/appdatas");

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
    public void testAdd() throws Exception {
        Config config = new Config();
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setCacheSize(100);
        config.setMsgAvgLen(10);
        config.setName("test");
        FileQueue<String> fq = new FileQueueImpl<String>(config);
        fq.add("ssss");
        Assert.assertEquals("ssss", fq.get());
        fq.close();
    }

    @Test
    public void testAddMultiFiles() throws Exception {
        Config config = new Config();
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setCacheSize(100);
        config.setMsgAvgLen(10);
        config.setName("test");
        config.setFileSiz(1024);
        FileQueue<Integer> fq = new FileQueueImpl<Integer>(config);
        int times = 1000;
        for (int i = 0; i < times; i++) {
            fq.add(i);
        }

        for (int i = 0; i < times; i++) {
            Assert.assertEquals(Integer.valueOf(i), fq.get());
        }
        fq.close();
    }

    @Test
    public void testGetTimeout() throws Exception {
        Config config = new Config();
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setCacheSize(100);
        config.setMsgAvgLen(10);
        config.setName("test");
        config.setFileSiz(1024);
        FileQueue<Integer> fq = new FileQueueImpl<Integer>(config);

        long start = System.currentTimeMillis();
        Integer res = fq.get(1, TimeUnit.SECONDS);
        Assert.assertEquals(1, (System.currentTimeMillis() - start) / 1000);
        Assert.assertNull(res);

    }

    @Test
    public void testQueueRestart() throws Exception {
        int times = 100;
        Config config = new Config();
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setCacheSize(100);
        config.setMsgAvgLen(10);
        config.setName("test");
        // single data file
        config.setFileSiz(1024 * 1024 * 1000);
        FileQueue<Integer> fq = new FileQueueImpl<Integer>(config);
        for (int i = 0; i < times; i++) {
            fq.add(i);
        }

        for (int i = 0; i < times / 2; i++) {
            Assert.assertEquals(Integer.valueOf(i), fq.get());
        }

        fq.close();

        fq = new FileQueueImpl<Integer>(config);
        for (int i = times / 2; i < times; i++) {
            Assert.assertEquals(Integer.valueOf(i), fq.get());
        }
    }

    @Test
    public void testQueueRestart2() throws Exception {
        int times = 1000;
        Config config = new Config();
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setCacheSize(100);
        config.setMsgAvgLen(10);
        config.setName("test");
        // multi data files
        config.setFileSiz(10);
        FileQueue<Integer> fq = new FileQueueImpl<Integer>(config);
        for (int i = 0; i < times; i++) {
            fq.add(i);
        }

        for (int i = 0; i < times / 2; i++) {
            Assert.assertEquals(Integer.valueOf(i), fq.get());
            System.out.println(i);
        }

        fq.close();
        Thread.sleep(5000);

        fq = new FileQueueImpl<Integer>(config);
        for (int i = times / 2; i < times; i++) {
            Assert.assertEquals(Integer.valueOf(i), fq.get());
            System.out.println(i);
        }

    }

    @Test
    public void testWriteSpeed() throws Exception {
        Config config = new Config();
        config.setMsgAvgLen(1024);
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setFileSiz(1024 * 1024 * 500);
        FileQueue<byte[]> fq = new FileQueueImpl<byte[]>(config);
        byte[] content = new byte[1024];
        for (int i = 0; i < 1024; i++) {
            content[i] = 0x55;
        }
        int times = 100000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
            fq.add(content);
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
        FileQueue<byte[]> fq = new FileQueueImpl<byte[]>(config);
        byte[] content = new byte[1024];
        for (int i = 0; i < 1024; i++) {
            content[i] = 0x55;
        }

        int times = 100000;
        for (int i = 0; i < times; i++) {
            fq.add(content);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
            fq.get();
        }
        System.out.println("[Read]Time spend " + (System.currentTimeMillis() - start) + "ms for " + times
                + " times. Avg msg length 1024bytes, each data file 500MB.");

    }

    @Test
    public void testReadWriteSpeed() throws Exception {
        Config config = new Config();
        config.setMsgAvgLen(1024);
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setFileSiz(1024 * 1024 * 500);
        FileQueue<byte[]> fq = new FileQueueImpl<byte[]>(config);
        byte[] content = new byte[1024];
        for (int i = 0; i < 1024; i++) {
            content[i] = 0x55;
        }

        int times = 10000;

        long start = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
            fq.add(content);
            fq.get();
        }
        System.out.println("[ReadWrite]Time spend " + (System.currentTimeMillis() - start) + "ms for " + times
                + " times. Avg msg length 1024bytes, each data file 500MB.");

    }
}
