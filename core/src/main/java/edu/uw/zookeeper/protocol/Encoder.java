package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;

import java.io.IOException;


public interface Encoder<I> {
    void encode(I input, ByteBuf output) throws IOException;
}
