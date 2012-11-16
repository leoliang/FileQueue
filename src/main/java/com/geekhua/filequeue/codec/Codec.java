package com.geekhua.filequeue.codec;

/**
 * @author Leo Liang
 * 
 */
public interface Codec<E> {

    byte[] encode(E element);

    E decode(byte[] bytes);
}
