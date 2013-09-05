package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

public interface Encodable {
    void encode(ByteBuf output) throws IOException;
}
