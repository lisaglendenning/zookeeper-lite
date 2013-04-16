package edu.uw.zookeeper.netty.protocol.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uw.zookeeper.netty.protocol.AnonymousHandler;
import edu.uw.zookeeper.protocol.FourLetterCommand;

@ChannelHandler.Sharable
public class FourLetterCommandEncoder extends
        MessageToMessageEncoder<FourLetterCommand> implements AnonymousHandler {

    protected static final Logger logger = LoggerFactory
            .getLogger(FourLetterCommandEncoder.class);

    public static FourLetterCommandEncoder create() {
        return new FourLetterCommandEncoder();
    }

    @Override
    protected Object encode(ChannelHandlerContext ctx, FourLetterCommand msg)
            throws Exception {
        if (logger.isTraceEnabled()) {
            logger.trace("Sending FourLetterCommand {} to {}", msg, ctx
                    .channel().remoteAddress());
        }
        ByteBuf buf = Unpooled.wrappedBuffer(msg.bytes());
        return buf;
    }
}