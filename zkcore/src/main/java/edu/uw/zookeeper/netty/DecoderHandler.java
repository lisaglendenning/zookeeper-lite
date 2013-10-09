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
import io.netty.handler.codec.ByteToMessageDecoder;

public class DecoderHandler extends ByteToMessageDecoder {

    public static DecoderHandler attach(
            Channel channel, Decoder<? extends Optional<?>> decoder, Logger logger) {
        DecoderHandler handler = create(decoder, logger);
        channel.pipeline().addFirst(DecoderHandler.class.getName(), handler);
        return handler;
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
    protected void decode(ChannelHandlerContext ctx, ByteBuf input,
            List<Object> output) throws IOException {
        while (input.isReadable()) {
            Optional<?> decoded = decoder.decode(input);
            if (decoded.isPresent()) {
                if (logger.isTraceEnabled()) {
                    logger.trace(LoggingMarker.NET_MARKER.get(), "DECODED {} ({})", decoded.get(), ctx.channel());
                }
                output.add(decoded.get());
            } else {
                break;
            }
        }
        input.discardSomeReadBytes();
    }
}
