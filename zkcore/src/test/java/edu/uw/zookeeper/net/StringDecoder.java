package edu.uw.zookeeper.net;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

public class StringDecoder implements Decoder<String, String> {

    @Override
    public Class<? extends String> decodeType() {
        return String.class;
    }

    @Override
    public String decode(ByteBuf input) throws IOException {
        int readable = input.readableBytes();
        byte[] bytes = new byte[readable];
        input.readBytes(bytes);
        return new String(bytes);
    }
}