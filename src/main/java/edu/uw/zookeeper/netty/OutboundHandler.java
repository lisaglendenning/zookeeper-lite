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

/**
 * 
 * "One limitation to keep in mind is that it is not possible to detect the handled message type of you specify I while instance your class. Because of this Netty does not allow to do so and will throw an Exception if you try. For this cases you should handle the type detection by your self by override the acceptOutboundMessage(Object) method and use Object as type parameter."
 * 
 * @param <I>
 */
public class OutboundHandler<I> extends ChannelOutboundMessageHandlerAdapter<Object> {
    
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
    public boolean acceptOutboundMessage(Object msg) throws Exception {
        return true;
    }
    
    @Override
    public void flush(ChannelHandlerContext ctx, Object message) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("Encoding {} ({})", message, ctx.channel());
        }
        @SuppressWarnings("unchecked")
        ByteBuf output = encoder.encode((I) message, ctx.alloc());
        
        if (logger.isTraceEnabled()) {
            logger.trace("Writing {} ({})", output, ctx.channel());
        }
        ChannelHandlerUtil.addToNextOutboundBuffer(ctx, output);
    }
}
