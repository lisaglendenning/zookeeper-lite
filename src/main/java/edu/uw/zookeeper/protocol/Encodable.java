package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;

public interface Encodable {
    ByteBuf encode(ByteBufAllocator output) throws IOException;
}
