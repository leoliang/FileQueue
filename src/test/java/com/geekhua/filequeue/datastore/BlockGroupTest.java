package com.geekhua.filequeue.datastore;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.geekhua.filequeue.utils.EncryptUtils;

/**
 * 
 * @author Leo Liang
 * 
 */
public class BlockGroupTest {
    private static final File   baseDir     = new File(System.getProperty("java.io.tmpdir", "."), "blockGroupTest");
    private static final byte[] HEADER      = new byte[] { (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAB };
    private static final int    CHECKSUMLEN = 20;

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
    public void testAllocate() {
        byte[] content = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        BlockGroup blockGroup = BlockGroup.allocate(content.length, 1024);
        blockGroup.setContent(content);
        Assert.assertEquals(1024, blockGroup.getBlockSize());
        Assert.assertEquals(1, blockGroup.getBlockCount());
        Assert.assertArrayEquals(contentBytes(content, 1024), blockGroup.array());
    }

    @Test
    public void testAllocateMultiBlocks() {
        byte[] content = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        BlockGroup blockGroup = BlockGroup.allocate(content.length, 10);
        blockGroup.setContent(content);
        Assert.assertEquals(10, blockGroup.getBlockSize());
        Assert.assertEquals(4, blockGroup.getBlockCount());
        Assert.assertArrayEquals(contentBytes(content, 10), blockGroup.array());
    }

    @Test
    public void testReadBlockGroupNormal() throws Exception {
        byte[] content = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        BlockGroup blockGroup = BlockGroup.allocate(content.length, 1024);
        blockGroup.setContent(content);

        RandomAccessFile file = getFile();
        file.write(blockGroup.array());
        file.seek(0);

        BlockGroup readBlockGroup = BlockGroup.read(file, 1024);
        Assert.assertArrayEquals(content, readBlockGroup.getContent());
    }

    @Test
    public void testReadBlockGroupNormalMultiBlocks() throws Exception {
        byte[] content = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        BlockGroup blockGroup = BlockGroup.allocate(content.length, 10);
        blockGroup.setContent(content);

        RandomAccessFile file = getFile();
        file.write(blockGroup.array());
        file.seek(0);

        BlockGroup readBlockGroup = BlockGroup.read(file, 10);
        Assert.assertArrayEquals(content, readBlockGroup.getContent());
    }

    @Test
    public void testReadBlockGroupFailByFirstBlock() throws Exception {
        ByteBuffer blocks = ByteBuffer.allocate(2048);
        blocks.put(new byte[] { 34, 5, 5, 66 });
        while (blocks.remaining() > 1024) {
            blocks.put((byte) 0);
        }
        byte[] content = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        blocks.put(contentBytes(content, 1024));

        RandomAccessFile file = getFile();
        file.write(blocks.array());
        file.seek(0);

        BlockGroup readBlockGroup = BlockGroup.read(file, 1024);
        Assert.assertArrayEquals(content, readBlockGroup.getContent());
    }

    @Test
    public void testReadBlockGroupFailByFirstNBlockMultiblock() throws Exception {
        ByteBuffer blocks = ByteBuffer.allocate(70);
        for (int i = 0; i < 3; i++) {
            blocks.put(new byte[] { 34, 5, 5, 66 });
            while (blocks.remaining() > 70 - (i + 1) * 10) {
                blocks.put((byte) 0);
            }
        }
        byte[] content = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        blocks.put(contentBytes(content, 10));

        RandomAccessFile file = getFile();
        file.write(blocks.array());
        file.seek(0);

        BlockGroup readBlockGroup = BlockGroup.read(file, 10);
        Assert.assertArrayEquals(content, readBlockGroup.getContent());
    }

    @Test
    public void testReadBlockGroupFailByCheckSum() throws Exception {
        ByteBuffer blocks = ByteBuffer.allocate(3072);
        blocks.put(new byte[] { 34, 5, 5, 66 });
        while (blocks.remaining() > 2048) {
            blocks.put((byte) 0);
        }
        byte[] content = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        blocks.put(contentBytesWithoutChecksum(content, 1024));

        blocks.put(contentBytes(content, 1024));

        RandomAccessFile file = getFile();
        file.write(blocks.array());
        file.seek(0);

        BlockGroup readBlockGroup = BlockGroup.read(file, 1024);
        Assert.assertArrayEquals(content, readBlockGroup.getContent());
    }

    @Test
    public void testReadBlockGroupFailByCheckSumMultiBlocks() throws Exception {
        ByteBuffer blocks = ByteBuffer.allocate(110);
        for (int i = 0; i < 3; i++) {
            blocks.put(new byte[] { 34, 5, 5, 66 });
            while (blocks.remaining() > 110 - (i + 1) * 10) {
                blocks.put((byte) 0);
            }
        }
        byte[] content = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        blocks.put(contentBytesWithoutChecksum(content, 10));

        blocks.put(contentBytes(content, 10));

        RandomAccessFile file = getFile();
        file.write(blocks.array());
        file.seek(0);

        BlockGroup readBlockGroup = BlockGroup.read(file, 10);
        Assert.assertArrayEquals(content, readBlockGroup.getContent());
    }

    @Test
    public void testReadBlockGroupFailByCheckSumMultiBlocks2() throws Exception {
        ByteBuffer blocks = ByteBuffer.allocate(110);
        for (int i = 0; i < 2; i++) {
            blocks.put(new byte[] { 34, 5, 5, 66 });
            while (blocks.remaining() > 110 - (i + 1) * 10) {
                blocks.put((byte) 0);
            }
        }

        blocks.put(HEADER);
        blocks.putInt(20);
        blocks.put(new byte[] { 0, 0 });

        byte[] content = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        blocks.put(contentBytesWithoutChecksum(content, 10));

        blocks.put(contentBytes(content, 10));

        RandomAccessFile file = getFile();
        file.write(blocks.array());
        file.seek(0);

        BlockGroup readBlockGroup = BlockGroup.read(file, 10);
        Assert.assertArrayEquals(content, readBlockGroup.getContent());
    }

    @Test
    public void testReadBlockGroupWithFlushDelay() throws Exception {
        ByteBuffer blocks = ByteBuffer.allocate(70);
        for (int i = 0; i < 2; i++) {
            blocks.put(new byte[] { 34, 5, 5, 66 });
            while (blocks.remaining() > 70 - (i + 1) * 10) {
                blocks.put((byte) 0);
            }
        }

        blocks.put(HEADER);
        blocks.putInt(20);
        blocks.put(new byte[] { 0, 0 });

        byte[] content = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        blocks.put(contentBytesWithoutChecksum(content, 10));

        byte[] bytes = contentBytes(content, 10);

        RandomAccessFile file = getFile();
        file.write(blocks.array());
        file.write(bytes, 0, 9);
        file.seek(0);

        BlockGroup readBlockGroup = BlockGroup.read(file, 10);
        Assert.assertNull(readBlockGroup);
        long originPos = file.getFilePointer();
        file.seek(file.length());
        file.write(bytes, 9, bytes.length - 9);
        file.seek(originPos);
        readBlockGroup = BlockGroup.read(file, 10);
        Assert.assertArrayEquals(content, readBlockGroup.getContent());
    }

    @Test
    public void testReadBlockGroupWithFlushDelay2() throws Exception {
        ByteBuffer blocks = ByteBuffer.allocate(70);
        for (int i = 0; i < 2; i++) {
            blocks.put(new byte[] { 34, 5, 5, 66 });
            while (blocks.remaining() > 70 - (i + 1) * 10) {
                blocks.put((byte) 0);
            }
        }

        blocks.put(HEADER);
        blocks.putInt(20);
        blocks.put(new byte[] { 0, 0 });

        byte[] content = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        blocks.put(contentBytesWithoutChecksum(content, 10));

        byte[] bytes = contentBytes(content, 10);

        RandomAccessFile file = getFile();
        file.write(blocks.array());
        file.write(bytes, 0, 12);
        file.seek(0);

        BlockGroup readBlockGroup = BlockGroup.read(file, 10);
        Assert.assertNull(readBlockGroup);
        long originPos = file.getFilePointer();
        file.seek(file.length());
        file.write(bytes, 12, bytes.length - 12);
        file.seek(originPos);
        readBlockGroup = BlockGroup.read(file, 10);
        Assert.assertArrayEquals(content, readBlockGroup.getContent());
    }

    private byte[] contentBytes(byte[] content, int blockSize) {
        int bytesLen = content.length + HEADER.length + 4 + CHECKSUMLEN;
        ByteBuffer expectedBytes = ByteBuffer.allocate(((bytesLen / blockSize) + (bytesLen % blockSize == 0 ? 0 : 1))
                * blockSize);
        expectedBytes.put(HEADER);
        expectedBytes.putInt(content.length + CHECKSUMLEN);
        expectedBytes.put(content);
        expectedBytes.put(EncryptUtils.sha1(content));
        while (expectedBytes.hasRemaining()) {
            expectedBytes.put((byte) 0);
        }
        return expectedBytes.array();
    }

    private byte[] contentBytesWithoutChecksum(byte[] content, int blockSize) {
        int bytesLen = content.length + HEADER.length + 4 + CHECKSUMLEN;
        ByteBuffer expectedBytes = ByteBuffer.allocate(((bytesLen / blockSize) + (bytesLen % blockSize == 0 ? 0 : 1))
                * blockSize);
        expectedBytes.put(HEADER);
        expectedBytes.putInt(content.length + CHECKSUMLEN);
        expectedBytes.put(content);
        while (expectedBytes.hasRemaining()) {
            expectedBytes.put((byte) 0);
        }
        return expectedBytes.array();
    }

    private RandomAccessFile getFile() throws Exception {
        return new RandomAccessFile(new File(baseDir, "test"), "rw");
    }

}
