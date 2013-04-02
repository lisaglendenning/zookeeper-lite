package org.apache.zookeeper.netty.protocol;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.handler.codec.DecoderException;

/*
 * Interprets the header as the length of the following frame
 * Processes one frame at a time
 */
public class FrameDecoder 
        extends ChannelInboundMessageHandlerAdapter<HeaderEvent> {

    public static FrameDecoder create() {
        return new FrameDecoder();
    }

    public class Callback implements FutureCallback<Void>, Callable<Void> {
        protected final HeaderEvent event;
        protected final ChannelHandlerContext ctx;
        protected final BufEvent msg;
        
        public Callback(HeaderEvent event, ChannelHandlerContext ctx, BufEvent msg) {
            this.event = checkNotNull(event);
            this.ctx = checkNotNull(ctx);
            this.msg = checkNotNull(msg);
            event.getBuf().retain();
            msg.getBuf().retain();
        }

        @Override
        public void onSuccess(Void thevoid) {
            ctx.executor().submit(this);
        }

        @Override
        public void onFailure(Throwable t) {
            msg.getCallback().onFailure(t);
        }

        @Override
        public Void call() throws Exception {
            ByteBuf frame = event.getBuf();
            if (logger.isDebugEnabled()) {
                if (frame.isReadable()) {
                    // something probably went wrong
                    logger.debug(String.format("%d unread bytes in frame from %s",
                            frame.readableBytes(), ctx.channel().remoteAddress()));
                }
            }
            int length = frame.capacity();
            frame.release();
            event.setHeader(null).setBuf(null).setCallback(null);
            ByteBuf buf = msg.getBuf();
            assert (buf.readableBytes() >= length);
            buf.skipBytes(length);
            buf.release();
            msg.getCallback().onSuccess(null);
            if (ctx.channel().config().isAutoRead()) {
                ctx.channel().read();
            }
            return null;
        }
    }
    
    protected final Logger logger = LoggerFactory.getLogger(FrameDecoder.class);
    protected HeaderEvent event;

    public FrameDecoder() {
        this.event = null;
    }

    @Override
    public void afterAdd(ChannelHandlerContext ctx) throws Exception {
        event = new HeaderEvent();
        super.afterAdd(ctx);
    }
    
    @Override
    public void afterRemove(ChannelHandlerContext ctx) throws Exception {
        event = null;
        super.afterRemove(ctx);
    }

    @Override
    public void channelReadSuspended(ChannelHandlerContext ctx) throws Exception {
        if (event.getHeader() == null) {
//            ctx.channel().read();
        }
        super.channelReadSuspended(ctx);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, 
            HeaderEvent msg)
            throws Exception {
        if (event.getHeader() != null) {
            // still processing the previous frame
            return;
        }

        // received sufficient bytes?
        int length = msg.getHeader().getInt(0);
        // sanity check
        if (length < 0) { // TODO max?
            throw new DecoderException();
        }
        
        ByteBuf in = msg.getBuf();
        if (in.readableBytes() >= length) {
            //in.retain();
            ByteBuf frame = in.slice(in.readerIndex(), length);
            //frame.retain();
            event.setHeader(msg.getHeader())
                .setBuf(frame)
                .setCallback(new Callback(event, ctx, msg));
            if (logger.isTraceEnabled()) {
                logger.trace("Received frame of length {} from {}", 
                        length, ctx.channel().remoteAddress());
            }

            ctx.nextInboundMessageBuffer().add(event);
        }
    }
}
