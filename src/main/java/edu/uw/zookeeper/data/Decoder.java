package edu.uw.zookeeper.data;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

public interface Decoder<O> {
    O decode(ByteBuf input) throws IOException;
}
