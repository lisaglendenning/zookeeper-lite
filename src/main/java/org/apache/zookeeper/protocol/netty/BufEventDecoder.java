package org.apache.zookeeper.protocol.netty;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;

import io.netty.buffer.BufType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundByteHandlerAdapter;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.channel.ChannelStateHandler;

public class BufEventDecoder extends ChannelInboundByteHandlerAdapter {

    public static class InboundBridge extends ChannelInboundMessageHandlerAdapter<ByteBuf> {

        @Override
        public void messageReceived(ChannelHandlerContext ctx, ByteBuf msg)
                throws Exception {
            ctx.nextInboundByteBuffer().writeBytes(msg);
            ctx.fireInboundBufferUpdated();
        }
        
    }
    
    public static BufEventDecoder create() {
        return new BufEventDecoder();
    }

    public class Callback implements FutureCallback<Void>, Callable<Void> {
        protected final ChannelHandlerContext ctx;
        
        public Callback(ChannelHandlerContext ctx) {
            this.ctx = checkNotNull(ctx);
        }

        @Override
        public void onSuccess(Void result) {
            ctx.executor().submit(this);
        }

        @Override
        public void onFailure(Throwable t) {
            try {
                ctx.handler().exceptionCaught(ctx, t);
            } catch (Exception e) {
                // TODO
                throw new AssertionError(t);
            }
        }

        @Override
        public Void call() throws Exception {
            // trigger a read event if there is more data to process
            // and we are not throttled
            if (ctx.channel().config().isAutoRead() && ctx.inboundByteBuffer().isReadable()) {
                ((ChannelStateHandler)(ctx.handler())).inboundBufferUpdated(ctx);
            }
            return null;
        }
    }

    protected final Logger logger = LoggerFactory.getLogger(BufEventDecoder.class);
    protected Callback callback;

    public BufEventDecoder() {
        this.callback = null;
    }

    @Override
    public void afterAdd(ChannelHandlerContext ctx) throws Exception {
        checkState(callback == null);
        callback = new Callback(ctx);
        if (ctx.channel().metadata().bufferType() == BufType.MESSAGE) {
            ctx.pipeline().addBefore(ctx.name(), InboundBridge.class.getName(), new InboundBridge());
        }
        super.afterAdd(ctx);
    }

    @Override
    public void afterRemove(ChannelHandlerContext ctx) throws Exception {
        checkState(callback != null);
        callback = null;
        super.afterRemove(ctx);
    }
    
    @Override
    public void inboundBufferUpdated(ChannelHandlerContext ctx, ByteBuf in)
            throws Exception {
        if (! in.isReadable()) {
            return;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Received {} bytes from {}", 
                    in.readableBytes(), ctx.channel().remoteAddress());
        }
        BufEvent event = new BufEvent()
            .setBuf(in)
            .setCallback(callback);
        ctx.nextInboundMessageBuffer().add(event);
        ctx.fireInboundBufferUpdated();
    }
}
