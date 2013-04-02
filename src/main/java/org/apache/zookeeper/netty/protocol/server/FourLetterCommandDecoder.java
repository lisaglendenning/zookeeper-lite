package org.apache.zookeeper.netty.protocol.server;

import org.apache.zookeeper.netty.protocol.AnonymousHandler;
import org.apache.zookeeper.netty.protocol.BufEvent;
import org.apache.zookeeper.protocol.FourLetterCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

/*
 * Determines whether incoming bytes from an anonymous client is a FourLetterWord
 * and if so, decodes it
 * Otherwise, passes it on
 * This handler should be removed from the pipeline when a client is
 * no longer anonymous
 */
@ChannelHandler.Sharable
public class FourLetterCommandDecoder 
    extends MessageToMessageDecoder<BufEvent>
    implements AnonymousHandler {

    public static FourLetterCommandDecoder create() {
        return new FourLetterCommandDecoder();
    }

    protected final Logger logger = LoggerFactory.getLogger(FourLetterCommandDecoder.class);

    @Override
    public Object decode(ChannelHandlerContext ctx, BufEvent msg)
            throws Exception {
        ByteBuf buf = msg.getBuf();
        int readableBytes = buf.readableBytes();
        Object result = decode(ctx, buf);
        if (buf.readableBytes() < readableBytes) {
            // consumed some bytes
            msg.getCallback().onSuccess(null);
        }
        if (result == buf) {
            result = msg;
        }
        return result;
    }

    public Object decode(ChannelHandlerContext ctx, ByteBuf msg)
            throws Exception {
        int length = FourLetterCommand.LENGTH;
        if (msg.readableBytes() < length) {
            // wait
            return null;
        }
        byte[] bytes = new byte[length];
        msg.markReaderIndex();
        msg.readBytes(bytes);
        if (FourLetterCommand.isWord(bytes)) {
            FourLetterCommand command = FourLetterCommand.fromWord(bytes);
            if (logger.isTraceEnabled()) {
                logger.trace("Received FourLetterCommand {} from {}", 
                        command, ctx.channel().remoteAddress());
            }
            // consumed some bytes
            return command;
        } else {
            // not a FourLetterWord
            // let the pipeline handle it
            msg.resetReaderIndex();
            return msg;
        }
    }
}
