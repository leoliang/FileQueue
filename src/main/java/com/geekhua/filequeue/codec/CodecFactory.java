package com.geekhua.filequeue.codec;

import com.geekhua.filequeue.Config.CodecType;

/**
 * @author Leo Liang
 * @author macro
 */
public class CodecFactory {

    private CodecFactory() {

    }

    public static <E> Codec<E> getInstance(CodecType codecType) {
        switch (codecType) {
        case KRYO:
            return new KryoCodec<E>();
        default:
            return new ObjectCodec<E>();
        }
    }
}
