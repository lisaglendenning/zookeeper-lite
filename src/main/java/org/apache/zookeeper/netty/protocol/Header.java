package org.apache.zookeeper.netty.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class Header {

    public static final int LENGTH = 4;
    
    public static ByteBuf alloc(ByteBufAllocator alloc) {
        int size = LENGTH;
        return alloc.buffer(size, size);
    }
}
