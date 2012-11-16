package com.geekhua.filequeue.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * 
 * @author Leo Liang
 * 
 */
public class ObjectCodec<E> implements Codec<E> {

    /*
     * (non-Javadoc)
     * 
     * @see com.geekhua.filequeue.codec.Codec#encode(java.lang.Object)
     */
    public byte[] encode(E element) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(element);
        } catch (IOException e) {
            return new byte[0];
        }
        return bos.toByteArray();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.geekhua.filequeue.codec.Codec#decode(byte[])
     */
    @SuppressWarnings("unchecked")
    public E decode(byte[] bytes) {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        try {
            ObjectInputStream ois = new ObjectInputStream(bis);
            return (E) ois.readObject();
        } catch (Exception e) {
            return null;
        }
    }

}
