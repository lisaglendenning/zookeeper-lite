package edu.uw.zookeeper.net;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

public interface Decoder<O,T> {
    Class<? extends T> decodeType();
    O decode(ByteBuf input) throws IOException;
}
