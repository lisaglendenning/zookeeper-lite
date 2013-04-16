package edu.uw.zookeeper.netty.protocol;

import io.netty.buffer.BufUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class HeaderEventEncoder extends MessageToMessageEncoder<HeaderEvent> {

    public static HeaderEventEncoder create() {
        return new HeaderEventEncoder();
    }

    protected final Logger logger = LoggerFactory
            .getLogger(HeaderEventEncoder.class);

    @Override
    protected Object encode(ChannelHandlerContext ctx, HeaderEvent msg)
            throws Exception {
        ByteBuf header = msg.getHeader();
        ByteBuf buf = msg.getBuf();
        if (logger.isTraceEnabled()) {
            String headerHex = BufUtil.hexDump(header);
            logger.trace("Sending header 0x{} to {}", headerHex, ctx.channel()
                    .remoteAddress());
        }
        CompositeByteBuf compositeBuf = ctx.alloc().compositeBuffer()
                .addComponents(header, buf);
        int readableBytes = header.readableBytes() + buf.readableBytes();
        compositeBuf.writerIndex(compositeBuf.writerIndex() + readableBytes);
        BufEvent event = new BufEvent().setBuf(compositeBuf).setCallback(
                msg.getCallback());
        return event;
    }
}