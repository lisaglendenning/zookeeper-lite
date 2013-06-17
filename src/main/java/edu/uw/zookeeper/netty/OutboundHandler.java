package edu.uw.zookeeper.netty;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uw.zookeeper.protocol.Encoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * TODO: check...
 * "One limitation to keep in mind is that it is not possible to detect the handled message type of you specify I while instance your class. Because of this Netty does not allow to do so and will throw an Exception if you try. For this cases you should handle the type detection by your self by override the acceptOutboundMessage(Object) method and use Object as type parameter."
 * 
 * @param <I>
 */
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
        this.logger = LoggerFactory.getLogger(getClass());
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
