package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;


public interface Encoder<I> {
    ByteBuf encode(I input, ByteBufAllocator output) throws IOException;
}
