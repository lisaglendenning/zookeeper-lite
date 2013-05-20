package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;

import java.io.IOException;

public abstract class Buffers {

    public static ByteBuf composite(ByteBufAllocator output, ByteBuf...bufs) throws IOException {
        CompositeByteBuf out = output.compositeBuffer(bufs.length);
        int length = 0;
        for (ByteBuf buf: bufs) {
            length += buf.readableBytes();
        }
        out.addComponents(bufs);
        out.writerIndex(out.writerIndex() + length);
        return out;
    }
    
    private Buffers() {}
}
