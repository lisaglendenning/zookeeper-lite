package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;

import edu.uw.zookeeper.net.Decoder;
import edu.uw.zookeeper.net.LoggingMarker;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;

public class DecoderHandler extends ByteToMessageDecoder {

    public static ChannelPipeline toPipeline(
            DecoderHandler handler,
            ChannelPipeline pipeline) {
        return pipeline.addFirst(DecoderHandler.class.getName(), handler);
    }
    
    public static DecoderHandler fromPipeline(ChannelPipeline pipeline) {
        // it seems that get by name may be faster than by class
        return (DecoderHandler) pipeline.get(DecoderHandler.class.getName());
    }
    
    public static DecoderHandler withDecoder(
            Decoder<? extends Optional<?>, ?> decoder,
            Logger logger) {
        return new DecoderHandler(decoder, logger);
    }

    private final Logger logger;
    private final Decoder<? extends Optional<?>, ?> decoder;

    protected DecoderHandler(
            Decoder<? extends Optional<?>, ?> decoder,
            Logger logger) {
        super();
        this.logger = checkNotNull(logger);
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
