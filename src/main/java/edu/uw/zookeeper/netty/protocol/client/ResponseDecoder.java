package edu.uw.zookeeper.netty.protocol.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import java.io.EOFException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

import edu.uw.zookeeper.SessionConnectionState;
import edu.uw.zookeeper.XidCounter;
import edu.uw.zookeeper.data.Operation;
import edu.uw.zookeeper.netty.protocol.BufEvent;
import edu.uw.zookeeper.protocol.client.SessionStateDecoder;
import edu.uw.zookeeper.util.Eventful;

public class ResponseDecoder extends
        MessageToMessageCodec<BufEvent, Operation.Request> {

    public static final String STATE_ATTRIBUTE_NAME = SessionStateDecoder.class
            .getName();
    public static final AttributeKey<SessionConnectionState> STATE_ATTRIBUTE_KEY = new AttributeKey<SessionConnectionState>(
            STATE_ATTRIBUTE_NAME);

    public static ResponseDecoder create(XidCounter xid, Eventful eventful) {
        return new ResponseDecoder(xid, eventful);
    }

    protected final Logger logger = LoggerFactory
            .getLogger(ResponseDecoder.class);
    protected final SessionStateDecoder decoder;

    @Inject
    protected ResponseDecoder(XidCounter xid, Eventful eventful) {
        this.decoder = SessionStateDecoder.create(eventful, xid);
    }

    public SessionConnectionState state() {
        return decoder.state();
    }

    @Override
    public void afterAdd(ChannelHandlerContext ctx) throws Exception {
        Attribute<SessionConnectionState> attr = ctx.channel().attr(
                STATE_ATTRIBUTE_KEY);
        attr.compareAndSet(null, state());
        super.afterAdd(ctx);
    }

    @Override
    public void beforeRemove(ChannelHandlerContext ctx) throws Exception {
        Attribute<SessionConnectionState> attr = ctx.channel().attr(
                STATE_ATTRIBUTE_KEY);
        attr.compareAndSet(state(), null);
        super.beforeRemove(ctx);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, BufEvent msg)
            throws Exception {
        Object response = decode(ctx, msg.getBuf());
        if (msg.getBuf().isReadable()) {
            throw new DecoderException(String.format(
                    "%d unexpected bytes after %s", msg.getBuf()
                            .readableBytes(), response.toString()));
        }
        msg.getCallback().onSuccess(null);
        return response;
    }

    protected Operation.Response decode(ChannelHandlerContext ctx, ByteBuf msg)
            throws Exception {
        InputStream stream = new ByteBufInputStream(msg);
        Operation.Response response;
        try {
            response = decoder.decode(stream);
        } catch (EOFException e) {
            throw new DecoderException(e);
        }
        stream.close();
        if (logger.isTraceEnabled()) {
            logger.trace(String.format("Response %s from %s", response, ctx
                    .channel().remoteAddress()));
        }
        return response;
    }

    @Override
    protected Object encode(ChannelHandlerContext ctx, Operation.Request msg)
            throws Exception {
        msg = decoder.apply(msg);
        return msg;
    }
}
