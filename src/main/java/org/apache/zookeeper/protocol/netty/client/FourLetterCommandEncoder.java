package org.apache.zookeeper.protocol.netty.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import org.apache.zookeeper.protocol.FourLetterCommand;
import org.apache.zookeeper.protocol.netty.AnonymousHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class FourLetterCommandEncoder 
    extends MessageToMessageEncoder<FourLetterCommand>
    implements AnonymousHandler {

    protected static final Logger logger = LoggerFactory.getLogger(FourLetterCommandEncoder.class);

    public static FourLetterCommandEncoder create() {
        return new FourLetterCommandEncoder();
    }

    @Override
    protected Object encode(ChannelHandlerContext ctx, FourLetterCommand msg)
            throws Exception {
        if (logger.isTraceEnabled()) {
            logger.trace("Sending FourLetterCommand {} to {}",
                    msg, ctx.channel().remoteAddress());
        }
        ByteBuf buf = Unpooled.wrappedBuffer(msg.bytes());
        return buf;
    }
}