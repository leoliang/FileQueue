package com.geekhua.filequeue.codec;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

public class CodecTest {
	private int MAX_OBJS = 100000;
	private int MAX_THREADS = 20;
	private int OBJS_PER_THREAD = MAX_OBJS / MAX_THREADS;
	private byte[][] objectBytes;
	private ExecutorService executors = Executors.newFixedThreadPool(MAX_THREADS);

	@Before
	public void init() {
		objectBytes = new byte[MAX_OBJS][];
	}
	@Test
	public void testDefaultSerializeEncode() {
		Codec codec = new ObjectCodec();
		for (int i = 0; i < MAX_OBJS; i++) {
			objectBytes[i] = codec.encode(new MyObject());
		}
	}

	@Test
	public void testDefaultSerializeDecode() {
		testDefaultSerializeEncode();
		Codec codec = new ObjectCodec();
		for (int i = 0; i < MAX_OBJS; i++) {
			codec.decode(objectBytes[i]);
		}
	}

	@Test
	public void testDefaultSerializeEncodeMultThreads() throws InterruptedException {
		final Codec codec = new ObjectCodec();
		for (int i = 0; i < MAX_THREADS; i++) {
			final int k = i;
			executors.submit(new Runnable() {
				public void run() {
					for (int j = 0; j < OBJS_PER_THREAD; j++) {
						objectBytes[k * OBJS_PER_THREAD + j] = codec.encode(new MyObject());
					}
				}
			});
		}
		executors.shutdown();
		executors.awaitTermination(100, TimeUnit.SECONDS);
	}

	@Test
	public void testDefaultSerializeDecodeMultThreads() throws InterruptedException {
		testDefaultSerializeEncode();
		final Codec codec = new ObjectCodec();
		for (int i = 0; i < MAX_THREADS; i++) {
			final int k = i;
			executors.submit(new Runnable() {
				public void run() {
					for (int j = 0; j < OBJS_PER_THREAD; j++) {
						codec.decode(objectBytes[k * OBJS_PER_THREAD + j]);
					}
				}
			});
		}
		executors.shutdown();
		executors.awaitTermination(100, TimeUnit.SECONDS);
	}

	@Test
	public void testKryoSerializeEncode() {
		Codec codec = new KryoCodec();
		for (int i = 0; i < MAX_OBJS; i++) {
			objectBytes[i] = codec.encode(new MyObject());
		}
	}

	@Test
	public void testKryoSerializeDecode() {
		testKryoSerializeEncode();
		Codec codec = new KryoCodec();
		for (int i = 0; i < MAX_OBJS; i++) {
			codec.decode(objectBytes[i]);
		}
	}

	@Test
	public void testKryoSerializeEncodeMultThreads() throws InterruptedException {
		final Codec codec = new KryoCodec();
		for (int i = 0; i < MAX_THREADS; i++) {
			final int k = i;
			executors.submit(new Runnable() {
				public void run() {
					for (int j = 0; j < OBJS_PER_THREAD; j++) {
						objectBytes[k * OBJS_PER_THREAD + j] = codec.encode(new MyObject());
					}
				}
			});
		}
		executors.shutdown();
		executors.awaitTermination(100, TimeUnit.SECONDS);
	}

	@Test
	public void testKryoSerializeDecodeMultThreads() throws InterruptedException {
		testKryoSerializeEncode();
		final Codec codec = new KryoCodec();
		for (int i = 0; i < MAX_THREADS; i++) {
			final int k = i;
			executors.submit(new Runnable() {
				public void run() {
					for (int j = 0; j < OBJS_PER_THREAD; j++) {
						codec.decode(objectBytes[k * OBJS_PER_THREAD + j]);
					}
				}
			});
		}
		executors.shutdown();
		executors.awaitTermination(100, TimeUnit.SECONDS);
	}

	public void clean() {
		objectBytes = null;
	}
}
