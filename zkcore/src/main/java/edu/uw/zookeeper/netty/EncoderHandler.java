package edu.uw.zookeeper.netty;

import java.io.IOException;

import org.apache.logging.log4j.Logger;

import edu.uw.zookeeper.net.LoggingMarker;
import edu.uw.zookeeper.protocol.Encoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToByteEncoder;

public class EncoderHandler<I> extends MessageToByteEncoder<I> {
    
    public static <I> EncoderHandler<I> attach(
            Channel channel, Class<I> type, Encoder<? super I> encoder, Logger logger) {
        EncoderHandler<I> handler = create(type, encoder, logger);
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addFirst(EncoderHandler.class.getName(), handler);
        return handler;
    }

    public static <I> EncoderHandler<I> create(
            Class<I> type, Encoder<? super I> encoder, Logger logger) {
        return new EncoderHandler<I>(type, encoder, logger);
    }

    protected final Logger logger;
    protected final Encoder<? super I> encoder;

    protected EncoderHandler(
            Class<I> type, Encoder<? super I> encoder, Logger logger) {
        super(type, true);
        this.logger = logger;
        this.encoder = encoder;
    }

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        // trigger a re-read of any accumulated buffers
        final ChannelPipeline pipeline = ctx.pipeline();
        DecoderHandler decoder = DecoderHandler.get(pipeline);
        assert (decoder != null);
        if (decoder.actualReadableBytes() > 0) {
            // this is hopefully probably safe ??
            ctx.channel().eventLoop().execute(new Runnable() {
                    @Override
                    public void run() {
                        pipeline.fireChannelRead(Unpooled.EMPTY_BUFFER);
                        pipeline.fireChannelReadComplete();
                    }
                });
        }
        
        super.read(ctx);
    }
    
    @Override
    protected void encode(ChannelHandlerContext ctx, I message, ByteBuf output) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace(LoggingMarker.NET_MARKER.get(), "ENCODING {} ({})", message, ctx.channel());
        }
        encoder.encode(message, output);
    }
}
