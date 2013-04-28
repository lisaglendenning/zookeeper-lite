package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;

import edu.uw.zookeeper.util.Publisher;

import io.netty.buffer.BufType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundByteHandlerAdapter;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;

public class InboundHandler extends ChannelInboundByteHandlerAdapter {

    @ChannelHandler.Sharable
    public static class MessageToByteHandler extends
            ChannelInboundMessageHandlerAdapter<ByteBuf> {
        @Override
        public void messageReceived(ChannelHandlerContext ctx, ByteBuf msg)
                throws Exception {
            ctx.nextInboundByteBuffer().writeBytes(msg);
            ctx.fireInboundBufferUpdated();
        }
    }

    private final Publisher publisher;

    public static InboundHandler newInstance(Publisher publisher) {
        return new InboundHandler(publisher);
    }

    private InboundHandler(Publisher publisher) {
        super();
        this.publisher = checkNotNull(publisher);
    }
    
    protected void post(Object event) {
        publisher.post(event);
    }

    @Override
    public void afterAdd(ChannelHandlerContext ctx) throws Exception {
        // I think that when the underlying channel type is
        // MESSAGE that our byte buffer will be skipped,
        // even if the message is a bytebuf
        if (ctx.channel().metadata().bufferType() == BufType.MESSAGE) {
            ctx.pipeline().addBefore(ctx.name(), MessageToByteHandler.class.getName(),
                    new MessageToByteHandler());
        }
        super.afterAdd(ctx);
    }

    @Override
    protected void inboundBufferUpdated(ChannelHandlerContext ctx, ByteBuf in)
            throws Exception {
        if (in.isReadable()) {
            post(in);
        }
    }
}
