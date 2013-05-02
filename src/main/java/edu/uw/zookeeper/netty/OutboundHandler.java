package edu.uw.zookeeper.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundMessageHandlerAdapter;

public class OutboundHandler extends ChannelOutboundMessageHandlerAdapter<ByteBuf> {
    
    public static OutboundHandler newInstance() {
        return new OutboundHandler();
    }
    
    private OutboundHandler() {
        super();
    }
    
    // TODO: since channel type doesn't change,
    // add a subclass handler depending on channel type
    @Override
    public void flush(ChannelHandlerContext ctx, ByteBuf msg) {
        switch (ctx.channel().metadata().bufferType()) {
        case BYTE:
            ctx.nextOutboundByteBuffer().writeBytes(msg);
            break;
        case MESSAGE:
            ctx.nextOutboundMessageBuffer().add(msg);
            break;
        default:
            throw new AssertionError();
        }
    }
}
