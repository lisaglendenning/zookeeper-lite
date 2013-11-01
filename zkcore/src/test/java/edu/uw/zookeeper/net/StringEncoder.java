package edu.uw.zookeeper.net;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

public class StringEncoder implements Encoder<String, String> {

    @Override
    public Class<? extends String> encodeType() {
        return String.class;
    }

    @Override
    public void encode(String input, ByteBuf output) throws IOException {
        output.writeBytes(input.getBytes());
    }
}