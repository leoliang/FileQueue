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
public class KryoCodec implements Codec {
	private ThreadLocal<Kryo> serializer = new ThreadLocal<Kryo>();
	private ThreadLocal<Output> output = new ThreadLocal<Output>();
	private ThreadLocal<Input> input = new ThreadLocal<Input>();
	public byte[] encode(Object element) {
		Kryo kryo = serializer.get();
		Output output = this.output.get();
		if (kryo == null) {
			kryo = new Kryo();
			kryo.setRegistrationRequired(false);
			serializer.set(kryo);
		}
		if (output == null) {
			output = new Output(1024, -1);
			this.output.set(output);
		}
		output.clear();
		kryo.writeClassAndObject(output, element);
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
		return kryo.readClassAndObject(input);
    }
}
