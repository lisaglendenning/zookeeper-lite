package edu.uw.zookeeper.netty;

import java.io.IOException;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import edu.uw.zookeeper.protocol.Encoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToByteEncoder;

public class OutboundHandler<I> extends MessageToByteEncoder<I> {
    
    public static <I> OutboundHandler<I> attach(Channel channel, Class<I> type, Encoder<? super I> encoder) {
        OutboundHandler<I> handler = newInstance(type, encoder);
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast(OutboundHandler.class.getName(), handler);
        return handler;
    }

    public static <I> OutboundHandler<I> newInstance(Class<I> type, Encoder<? super I> encoder) {
        return new OutboundHandler<I>(type, encoder);
    }

    protected final Logger logger;
    protected final Encoder<? super I> encoder;

    protected OutboundHandler(Class<I> type, Encoder<? super I> encoder) {
        super(type, true);
        this.logger = LogManager.getLogger(getClass());
        this.encoder = encoder;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, I message, ByteBuf output) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("Encoding {} ({})", message, ctx.channel());
        }
        encoder.encode(message, output);
    }
}
