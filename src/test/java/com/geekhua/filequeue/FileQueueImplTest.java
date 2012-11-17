package com.geekhua.filequeue;

import java.io.File;

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
        }

        fq.close();

        fq = new FileQueueImpl<Integer>(config);
        for (int i = times / 2; i < times; i++) {
            Assert.assertEquals(Integer.valueOf(i), fq.get());
        }
    }
}
