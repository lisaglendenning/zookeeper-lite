package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.apache.logging.log4j.Logger;

import edu.uw.zookeeper.net.Encoder;
import edu.uw.zookeeper.net.LoggingMarker;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToByteEncoder;

public class EncoderHandler<I> extends MessageToByteEncoder<I> {
    
    public static ChannelPipeline toPipeline(
            EncoderHandler<?> handler,
            ChannelPipeline pipeline) {
        return pipeline.addFirst(EncoderHandler.class.getName(), handler);
    }

    public static EncoderHandler<?> fromPipeline(ChannelPipeline pipeline) {
        return (EncoderHandler<?>) pipeline.get(EncoderHandler.class.getName());
    }

    public static <I> EncoderHandler<I> withEncoder(
            Encoder<I, ? extends I> encoder, Logger logger) {
        return new EncoderHandler<I>(encoder.encodeType(), encoder, logger);
    }

    private final Logger logger;
    private final Encoder<? super I, ?> encoder;

    protected EncoderHandler(
            Class<? extends I> type, Encoder<? super I, ?> encoder, Logger logger) {
        super(type, true);
        this.logger = checkNotNull(logger);
        this.encoder = checkNotNull(encoder);
    }

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        // trigger a re-read of any accumulated buffers
        final ChannelPipeline pipeline = ctx.pipeline();
        DecoderHandler decoder = DecoderHandler.fromPipeline(pipeline);
        if ((decoder != null) && (decoder.actualReadableBytes() > 0)) {
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
