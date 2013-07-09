package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import com.google.common.base.Optional;

public abstract class ForwardingProtocolCodec<I,O> implements ProtocolCodec<I,O> {
    
    @Override
    public void encode(I input, ByteBuf output) throws IOException {
        delegate().encode(input, output);
    }

    @Override
    public Optional<O> decode(ByteBuf input) throws IOException {
        return delegate().decode(input);
    }

    @Override
    public ProtocolState state() {
        return delegate().state();
    }

    @Override
    public void register(Object handler) {
        delegate().register(handler);
    }

    @Override
    public void unregister(Object handler) {
        delegate().unregister(handler);
    }

    @Override
    public String toString() {
        return delegate().toString();
    }
    
    protected abstract ProtocolCodec<I,O> delegate();
}
