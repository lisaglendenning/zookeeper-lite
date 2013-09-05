package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

public interface Decoder<O> {
    O decode(ByteBuf input) throws IOException;
}
