package com.geekhua.filequeue.codec;

import org.objenesis.strategy.StdInstantiatorStrategy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * 
 * @author macro
 * 
 * @param <E>
 */
public class KryoCodec implements Codec {
	private ThreadLocal<Kryo> serializer = new ThreadLocal<Kryo>();
	public byte[] encode(Object element) {
		Kryo kryo = serializer.get();
		if (kryo == null) {
			kryo = new Kryo();
			kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
			kryo.setRegistrationRequired(false);
			serializer.set(kryo);
		}
        Output output = new Output(1024, -1);
        kryo.writeClassAndObject(output, element);
        return output.toBytes();
    }

	public Object decode(byte[] bytes) {
		Kryo kryo = serializer.get();
		if (kryo == null) {
			kryo = new Kryo();
			kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
			kryo.setRegistrationRequired(false);
			serializer.set(kryo);
		}
		kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        Input input = new Input();
        input.setBuffer(bytes);
		return kryo.readClassAndObject(input);
    }
}
