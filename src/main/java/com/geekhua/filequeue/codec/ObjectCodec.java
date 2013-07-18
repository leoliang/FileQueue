package com.geekhua.filequeue.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Leo Liang
 * 
 */
public class ObjectCodec implements Codec {
    private static final Logger log = LoggerFactory.getLogger(ObjectCodec.class);

    public byte[] encode(Object element) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(element);
        } catch (IOException e) {
            log.warn("Encode object({}) fail", element);
            return new byte[0];
        }
        return bos.toByteArray();
    }

    public Object decode(byte[] bytes) {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        try {
            ObjectInputStream ois = new ObjectInputStream(bis);
            return ois.readObject();
        } catch (Exception e) {
			log.warn("Decode object({}) fail", Arrays.toString(bytes));
            return null;
        }
    }

}
