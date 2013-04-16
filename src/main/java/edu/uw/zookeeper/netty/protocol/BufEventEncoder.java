package edu.uw.zookeeper.netty.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundMessageHandlerAdapter;

@ChannelHandler.Sharable
public class BufEventEncoder extends
        ChannelOutboundMessageHandlerAdapter<BufEvent> {

    public static BufEventEncoder create() {
        return new BufEventEncoder();
    }

    protected final Logger logger = LoggerFactory
            .getLogger(BufEventEncoder.class);

    @Override
    public void flush(ChannelHandlerContext ctx, BufEvent msg) throws Exception {
        if (logger.isTraceEnabled()) {
            logger.trace("Sending {} bytes to {}",
                    msg.getBuf().readableBytes(), ctx.channel().remoteAddress());
        }
        ctx.nextOutboundMessageBuffer().add(msg.getBuf());
        ctx.flush().addListener(new ChannelFutureCallback(msg.getCallback()));
    }
}
