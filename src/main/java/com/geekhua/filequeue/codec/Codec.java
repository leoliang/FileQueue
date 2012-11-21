package com.geekhua.filequeue.codec;

/**
 * @author Leo Liang
 * 
 */
public interface Codec {

    byte[] encode(Object element);

    Object decode(byte[] bytes);
}
