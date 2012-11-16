package com.geekhua.filequeue.codec;

import com.geekhua.filequeue.Config.CodecType;

/**
 * @author Leo Liang
 * 
 */
public class CodecFactory {

    private CodecFactory() {

    }

    public static <E> Codec<E> getInstance(CodecType codecType) {
        return new ObjectCodec<E>();
    }
}
