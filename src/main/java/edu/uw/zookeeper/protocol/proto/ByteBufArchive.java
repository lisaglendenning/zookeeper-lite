package edu.uw.zookeeper.protocol.proto;

import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

import edu.uw.zookeeper.common.Reference;

public abstract class ByteBufArchive implements Reference<ByteBuf> {

    protected final static Charset CHARSET = Charset.forName("UTF-8");
    
    protected final ByteBuf buffer;
    
    public ByteBufArchive(ByteBuf buffer) {
        this.buffer = buffer;
    }

    @Override
    public ByteBuf get() {
        return buffer;
    }
}
