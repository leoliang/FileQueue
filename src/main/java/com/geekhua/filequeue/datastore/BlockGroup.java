package com.geekhua.filequeue.datastore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.apache.commons.lang.ArrayUtils;

import com.geekhua.filequeue.utils.EncryptUtils;
import com.geekhua.filequeue.utils.StreamUtils;

/**
 * @author Leo Liang
 * 
 */
public class BlockGroup {
    public static final byte[] HEADER      = new byte[] { (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAB };
    public static final int    CHECKSUMLEN = 20;

    private ByteBuffer         data;
    private int                blockSize;
    private byte[]             content;
    private int                blockCount;

    private BlockGroup(ByteBuffer data, int blockSize, int blockCount) {
        this.data = data;
        this.blockSize = blockSize;
        this.blockCount = blockCount;
    }

    public byte[] getContent() {
        return content;
    }

    public int getBlockCount() {
        return blockCount;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public static int estimateBlockSize(int contentSize) {
        return contentSize + HEADER.length + 4 + CHECKSUMLEN;
    }

    public static BlockGroup allocate(int contentSize, int blockSize) {
        int blockCount = getBlockCount(contentSize + CHECKSUMLEN, blockSize);
        ByteBuffer data = ByteBuffer.allocate(blockCount * blockSize);

        BlockGroup blockGroup = new BlockGroup(data, blockSize, blockCount);
        if (data.remaining() >= 4) {
            data.put(HEADER);
        }
        data.putInt(contentSize + CHECKSUMLEN);
        return blockGroup;
    }

    private static int getBlockCount(int contentLength, int blockSize) {
        int dataLen = contentLength + HEADER.length + 4;
        return dataLen / blockSize + (dataLen % blockSize == 0 ? 0 : 1);
    }

    public static BlockGroup read(RandomAccessFile file, int blockSize) throws IOException {
        long markedPos = file.getFilePointer();
        byte[] block = new byte[blockSize];
        if (file.length() - file.getFilePointer() < blockSize) {
            return null;
        }
        StreamUtils.readFully(file, block, 0, blockSize);
        if (validateHeader(block)) {
            ByteBuffer blockBuffer = ByteBuffer.wrap(block);
            blockBuffer.position(HEADER.length);
            int contentAndChecksumLen = blockBuffer.getInt();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            baos.write(block);
            int unreadBlockCount = getBlockCount(contentAndChecksumLen, blockSize) - 1;
            for (int i = 0; i < unreadBlockCount; i++) {
                if (file.length() - file.getFilePointer() < blockSize) {
                    file.seek(markedPos);
                    return null;
                }
                StreamUtils.readFully(file, block, 0, blockSize);
                baos.write(block);
            }

            byte[] data = baos.toByteArray();
            if (validateChecksum(data, contentAndChecksumLen)) {
                BlockGroup blockGroup = BlockGroup.allocate(contentAndChecksumLen - CHECKSUMLEN, blockSize);
                byte[] content = new byte[contentAndChecksumLen - CHECKSUMLEN];
                System.arraycopy(data, HEADER.length + 4, content, 0, content.length);
                blockGroup.setContent(content);
                return blockGroup;
            }

        }
        file.seek(markedPos + blockSize);
        return read(file, blockSize);
    }

    private static boolean validateHeader(byte[] block) {
        if (block != null && block.length >= HEADER.length) {
            for (int i = 0; i < HEADER.length; i++) {
                if (block[i] != HEADER[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private static boolean validateChecksum(byte[] block, int contentAndChecksumLen) {
        if (block != null && block.length >= contentAndChecksumLen + HEADER.length + 4) {
            byte[] content = new byte[contentAndChecksumLen - CHECKSUMLEN];
            byte[] checksum = new byte[CHECKSUMLEN];
            System.arraycopy(block, HEADER.length + 4, content, 0, content.length);
            System.arraycopy(block, HEADER.length + 4 + content.length, checksum, 0, checksum.length);
            byte[] contentChecksum = EncryptUtils.sha1(content);
            if (contentChecksum.length == CHECKSUMLEN) {
                if (ArrayUtils.isEquals(contentChecksum, checksum)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void setContent(byte[] content) {
        if (content != null && data.remaining() >= content.length) {
            data.put(content);
            this.content = content;
        }
    }

    public byte[] array() {
        if (data.remaining() >= CHECKSUMLEN) {
            data.put(EncryptUtils.sha1(content));
            return data.array();
        }
        return new byte[0];
    }
}
