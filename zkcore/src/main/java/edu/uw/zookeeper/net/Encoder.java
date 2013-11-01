package edu.uw.zookeeper.net;

import io.netty.buffer.ByteBuf;

import java.io.IOException;


public interface Encoder<I,T> {
    Class<? extends T> encodeType();
    void encode(I input, ByteBuf output) throws IOException;
}
