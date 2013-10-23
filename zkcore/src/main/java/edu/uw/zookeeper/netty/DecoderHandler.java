package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;

import edu.uw.zookeeper.net.LoggingMarker;
import edu.uw.zookeeper.protocol.Decoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;

public class DecoderHandler extends ByteToMessageDecoder {

    public static DecoderHandler attach(
            Channel channel, Decoder<? extends Optional<?>> decoder, Logger logger) {
        DecoderHandler handler = create(decoder, logger);
        channel.pipeline().addFirst(DecoderHandler.class.getName(), handler);
        return handler;
    }
    
    public static DecoderHandler get(ChannelPipeline pipeline) {
        // it seems that get by name may be faster than by class
        return (DecoderHandler) pipeline.get(DecoderHandler.class.getName());
    }
    
    public static DecoderHandler create(
            Decoder<? extends Optional<?>> decoder,
            Logger logger) {
        return new DecoderHandler(decoder, logger);
    }

    protected final Logger logger;
    protected final Decoder<? extends Optional<?>> decoder;

    protected DecoderHandler(
            Decoder<? extends Optional<?>> decoder,
            Logger logger) {
        super();
        this.logger = logger;
        this.decoder = checkNotNull(decoder);
    }
    
    @Override
    public int actualReadableBytes() {
        // TODO: check that this is safe to expose...
        return super.actualReadableBytes();
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf input,
            List<Object> output) throws IOException {
        if (input.isReadable()) {
            Optional<?> decoded = decoder.decode(input);
            if (decoded.isPresent()) {
                Object message = decoded.get();
                if (logger.isTraceEnabled()) {
                    logger.trace(LoggingMarker.NET_MARKER.get(), "DECODED {} ({})", message, ctx.channel());
                }
                output.add(message);
            }
        }
    }
}
