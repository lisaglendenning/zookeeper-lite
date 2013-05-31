package edu.uw.zookeeper.netty;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uw.zookeeper.protocol.Encoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundMessageHandlerAdapter;
import io.netty.channel.ChannelPipeline;

public class OutboundHandler<I> extends ChannelOutboundMessageHandlerAdapter<I> {
    
    public static <I> OutboundHandler<I> attach(Channel channel, Encoder<? super I> encoder) {
        OutboundHandler<I> handler = newInstance(encoder);
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast(OutboundHandler.class.getName(), handler);
        return handler;
    }

    public static <I> OutboundHandler<I> newInstance(Encoder<? super I> encoder) {
        return new OutboundHandler<I>(encoder);
    }

    protected final Logger logger;
    protected final Encoder<? super I> encoder;

    protected OutboundHandler(Encoder<? super I> encoder) {
        super();
        this.logger = LoggerFactory.getLogger(getClass());
        this.encoder = encoder;
    }
    
    @Override
    public void flush(ChannelHandlerContext ctx, I message) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("Encoding {} ({})", message, ctx);
        }
        ByteBuf output = encoder.encode(message, ctx.alloc());
        
        if (logger.isTraceEnabled()) {
            logger.trace("Writing {} ({})", output, ctx);
        }
        ChannelHandlerUtil.addToNextOutboundBuffer(ctx, output);
    }
}
