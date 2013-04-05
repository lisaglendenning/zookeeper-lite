package org.apache.zookeeper.netty.protocol.server;

import java.io.EOFException;
import java.io.InputStream;
import org.apache.zookeeper.SessionConnectionState;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.data.Operation;
import org.apache.zookeeper.netty.protocol.BufEvent;
import org.apache.zookeeper.protocol.server.SessionStateDecoder;
import org.apache.zookeeper.util.Eventful;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;


public class RequestDecoder
        extends MessageToMessageCodec<BufEvent, Operation.Response> {

    public static final String STATE_ATTRIBUTE_NAME = 
            SessionStateDecoder.class.getName();
    public static final AttributeKey<SessionConnectionState> STATE_ATTRIBUTE_KEY = 
            new AttributeKey<SessionConnectionState>(STATE_ATTRIBUTE_NAME);

    public static RequestDecoder create(Zxid zxid, Eventful eventful) {
        return new RequestDecoder(zxid, eventful);
    }

    protected final Logger logger = LoggerFactory.getLogger(RequestDecoder.class);
    protected final SessionStateDecoder decoder;
    
    @Inject
    protected RequestDecoder(Zxid zxid, Eventful eventful) {
        this.decoder = SessionStateDecoder.create(zxid, eventful);
    }
    
    public SessionConnectionState state() {
        return decoder.state();
    }

    @Override
    public void afterAdd(ChannelHandlerContext ctx) throws Exception {
        Attribute<SessionConnectionState> attr = ctx.channel().attr(STATE_ATTRIBUTE_KEY);
        attr.compareAndSet(null, state());
        super.afterAdd(ctx);
    }
    
    @Override
    public void beforeRemove(ChannelHandlerContext ctx) throws Exception {
        Attribute<SessionConnectionState> attr = ctx.channel().attr(STATE_ATTRIBUTE_KEY);
        attr.compareAndSet(state(), null);
        super.beforeRemove(ctx);
    }

    @Override
    public void channelReadSuspended(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().config().isAutoRead()) {
            super.channelReadSuspended(ctx);
        }
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, BufEvent msg)
            throws Exception {
        Operation.Request request = decode(ctx, msg.getBuf());
        if (msg.getBuf().isReadable()) {
            throw new DecoderException(String.format("%d unexpected bytes after %s",
                    msg.getBuf().readableBytes(), request.toString()));
        }
        msg.getCallback().onSuccess(null);
        return request;
    }
    
    protected Operation.Request decode(ChannelHandlerContext ctx, ByteBuf msg)
            throws Exception {
        InputStream stream = new ByteBufInputStream(msg);
        Operation.Request request;
        try {
            request = decoder.decode(stream);
        } catch (EOFException e) {
            throw new DecoderException(e);
        }
        stream.close();
        if (logger.isTraceEnabled()) {
            logger.trace("Received {} from {}", 
                    request,
                    ctx.channel().remoteAddress());
        }
        return request;
    }

    @Override
    protected Object encode(ChannelHandlerContext ctx, Operation.Response msg)
            throws Exception {
        msg = decoder.apply(msg);
        return msg;
    }
    
}
