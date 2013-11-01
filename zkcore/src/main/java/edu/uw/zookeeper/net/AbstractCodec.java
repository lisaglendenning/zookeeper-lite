package edu.uw.zookeeper.net;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import com.google.common.base.Optional;

public abstract class AbstractCodec<I,O,U,V> implements Codec<I,O,U,V> {

    @Override
    public Class<? extends U> encodeType() {
        return encoder().encodeType();
    }

    @Override
    public void encode(I input, ByteBuf output) throws IOException {
        encoder().encode(input, output);
    }

    @Override
    public Class<? extends V> decodeType() {
        return decoder().decodeType();
    }

    @Override
    public Optional<O> decode(ByteBuf input) throws IOException {
        return Optional.<O>fromNullable(decoder().decode(input));
    }

    protected abstract Encoder<? super I,U> encoder();
    
    protected abstract Decoder<? extends O,V> decoder();
}
