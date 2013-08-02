package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;

import edu.uw.zookeeper.protocol.Decoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class DecoderHandler<O> extends ByteToMessageDecoder {

    public static <O> DecoderHandler<O> attach(
            Channel channel, Decoder<Optional<O>> decoder, Logger logger) {
        DecoderHandler<O> handler = create(decoder, logger);
        channel.pipeline().addFirst(DecoderHandler.class.getName(), handler);
        return handler;
    }
    
    public static <O> DecoderHandler<O> create(
            Decoder<Optional<O>> decoder,
            Logger logger) {
        return new DecoderHandler<O>(decoder, logger);
    }

    protected final Logger logger;
    protected final Decoder<Optional<O>> decoder;

    protected DecoderHandler(
            Decoder<Optional<O>> decoder,
            Logger logger) {
        super();
        this.logger = logger;
        this.decoder = checkNotNull(decoder);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf input,
            List<Object> output) throws IOException {
        while (input.isReadable()) {
            O out = decoder.decode(input).orNull();
            if (out != null) {
                if (logger.isTraceEnabled()) {
                    logger.trace(Logging.NETTY_MARKER, "DECODED {} ({})", out, ctx.channel());
                }
                output.add(out);
            } else {
                break;
            }
        }
    }
}
