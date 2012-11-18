package com.geekhua.filequeue.codec;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * 
 * @author macro
 * 
 * @param <E>
 */
public class KryoCodec<E> implements Codec<E> {
    public byte[] encode(E element) {
        Kryo kryo = new Kryo();
        Output output = new Output(1024, -1);
        kryo.writeClassAndObject(output, element);
        return output.toBytes();
    }

    @SuppressWarnings("unchecked")
    public E decode(byte[] bytes) {
        Kryo kryo = new Kryo();
        Input input = new Input();
        input.setBuffer(bytes);
        return (E) kryo.readClassAndObject(input);
    }
}
