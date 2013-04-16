package edu.uw.zookeeper.netty.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;

@ChannelHandler.Sharable
public class LoggingDecoder extends ChannelInboundMessageHandlerAdapter<Object> {

    public static LoggingDecoder create() {
        return new LoggingDecoder();
    }

    protected final Logger logger;

    public LoggingDecoder() {
        this(LoggerFactory.getLogger(LoggingDecoder.class));
    }

    public LoggingDecoder(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, Object msg)
            throws Exception {
        logger.debug("Received {} from {}", msg, ctx.channel().remoteAddress());
        ctx.nextInboundMessageBuffer().add(msg);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event)
            throws Exception {
        logger.debug("Event {} on {}", event, ctx.channel().remoteAddress());
        super.userEventTriggered(ctx, event);
    }
}
