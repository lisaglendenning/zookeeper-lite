package edu.uw.zookeeper.netty.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class FrameEncoder extends MessageToMessageEncoder<BufEvent> {

    public static FrameEncoder create() {
        return new FrameEncoder();
    }

    protected final Logger logger = LoggerFactory.getLogger(FrameEncoder.class);

    public FrameEncoder() {
    }

    @Override
    protected Object encode(ChannelHandlerContext ctx, BufEvent msg)
            throws Exception {
        ByteBuf buf = msg.getBuf();
        int length = buf.readableBytes();
        if (logger.isTraceEnabled()) {
            logger.trace("Sending frame of length {} to {}", length, ctx
                    .channel().remoteAddress());
        }
        ByteBuf header = Header.alloc(ctx.alloc());
        header.writeInt(length);
        @SuppressWarnings("unchecked")
        CallbackList<Void> callback = new CallbackList<Void>(new BufCallback(
                buf), msg.getCallback());
        HeaderEvent event = new HeaderEvent().setHeader(header).setBuf(buf)
                .setCallback(callback);
        return event;
    }
}