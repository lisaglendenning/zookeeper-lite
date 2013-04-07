package org.apache.zookeeper.netty.protocol;

import java.io.OutputStream;

import org.apache.zookeeper.netty.protocol.BufCallback;
import org.apache.zookeeper.netty.protocol.BufEvent;
import org.apache.zookeeper.protocol.Encodable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundMessageHandlerAdapter;
import io.netty.handler.codec.CodecException;

@ChannelHandler.Sharable
public class EncodableEncoder extends
        ChannelOutboundMessageHandlerAdapter<Encodable> {

    protected static final Logger logger = LoggerFactory
            .getLogger(EncodableEncoder.class);

    public static EncodableEncoder create() {
        return new EncodableEncoder();
    }

    @Override
    public void flush(ChannelHandlerContext ctx, Encodable msg)
            throws Exception {
        Object encoded = encode(ctx, msg);
        ctx.nextOutboundMessageBuffer().unfoldAndAdd(encoded);
    }

    protected Object encode(ChannelHandlerContext ctx, Encodable msg)
            throws Exception {
        ByteBuf out = ctx.alloc().buffer();
        try {
            encode(ctx, msg, out);
        } catch (CodecException e) {
            throw e;
        } catch (Exception e) {
            throw new CodecException(e);
        }

        BufEvent event = new BufEvent().setBuf(out).setCallback(
                new BufCallback(out));
        return event;
    }

    protected void encode(ChannelHandlerContext ctx, Encodable msg, ByteBuf out)
            throws Exception {
        OutputStream stream = msg.encode(new ByteBufOutputStream(out));
        stream.close();
    }
}
