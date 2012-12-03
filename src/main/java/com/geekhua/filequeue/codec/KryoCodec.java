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
	private ThreadLocal<Output> output = new ThreadLocal<Output>();
	private ThreadLocal<Input> input = new ThreadLocal<Input>();
	private Class<?> type;
	public byte[] encode(Object element) {
		if (type==null){
			type = element.getClass();
		}
		Kryo kryo = serializer.get();
		Output output = this.output.get();
		if (kryo == null) {
			kryo = new Kryo();
			kryo.setRegistrationRequired(false);
			kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
			serializer.set(kryo);
		}
		if (output == null) {
			output = new Output(1024, -1);
			this.output.set(output);
		}
		output.clear();
		kryo.writeObject(output, element);
        return output.toBytes();
    }

	public Object decode(byte[] bytes) {
		Kryo kryo = serializer.get();
		Input input = this.input.get();
		if (kryo == null) {
			kryo = new Kryo();
			kryo.setRegistrationRequired(false);
			serializer.set(kryo);
		}
		if (input == null) {
			input = new Input();
			this.input.set(input);
		}
        input.setBuffer(bytes);
		return kryo.readObject(input,type);
//		return kryo.readClassAndObject(input);
    }
}
