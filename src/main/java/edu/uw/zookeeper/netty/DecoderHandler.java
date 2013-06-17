package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import edu.uw.zookeeper.protocol.Decoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.MessageList;
import io.netty.handler.codec.ByteToMessageDecoder;

public class DecoderHandler<O> extends ByteToMessageDecoder {

    public static <O> DecoderHandler<O> attach(Channel channel, Decoder<Optional<O>> decoder) {
        DecoderHandler<O> handler = newInstance(decoder);
        channel.pipeline().addFirst(DecoderHandler.class.getName(), handler);
        return handler;
    }
    
    public static <O> DecoderHandler<O> newInstance(Decoder<Optional<O>> decoder) {
        return new DecoderHandler<O>(decoder);
    }

    protected final Logger logger;
    protected final Decoder<Optional<O>> decoder;

    protected DecoderHandler(Decoder<Optional<O>> decoder) {
        super();
        this.logger = LoggerFactory.getLogger(getClass());
        this.decoder = checkNotNull(decoder);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf input,
            MessageList<Object> output) throws IOException {
        while (input.isReadable()) {
            O out = decoder.decode(input).orNull();
            if (out != null) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Read {} ({})", out, ctx.channel());
                }
                output.add(out);
            } else {
                break;
            }
        }
    }
}
