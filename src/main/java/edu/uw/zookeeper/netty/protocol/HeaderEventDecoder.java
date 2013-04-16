package edu.uw.zookeeper.netty.protocol;

import static com.google.common.base.Preconditions.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;

import io.netty.buffer.BufUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;

/*
 * Waits for a fixed-size header in the byte stream
 * All code must be called from the same thread for the same channel
 * Processes one header at a time
 */
public class HeaderEventDecoder extends
        ChannelInboundMessageHandlerAdapter<BufEvent> {

    public static HeaderEventDecoder create() {
        return new HeaderEventDecoder();
    }

    public class Callback implements FutureCallback<Void> {
        protected final HeaderEvent event;
        protected final FutureCallback<Void> callback;

        public Callback(HeaderEvent event, FutureCallback<Void> callback) {
            this.event = checkNotNull(event);
            this.callback = checkNotNull(callback);
        }

        @Override
        public void onSuccess(Void result) {
            if (event.getCallback() == this) {
                event.getHeader().clear();
                event.setBuf(null).setCallback(null);
            }
            callback.onSuccess(result);
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
    }

    protected final Logger logger = LoggerFactory
            .getLogger(HeaderEventDecoder.class);
    protected HeaderEvent event;

    public HeaderEventDecoder() {
        this.event = null;
    }

    @Override
    public void afterAdd(ChannelHandlerContext ctx) throws Exception {
        checkState(event == null);
        event = new HeaderEvent();
        super.afterAdd(ctx);
    }

    @Override
    public void afterRemove(ChannelHandlerContext ctx) throws Exception {
        checkState(event != null);
        event = null;
        super.afterRemove(ctx);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, BufEvent msg)
            throws Exception {
        ByteBuf header = event.getHeader();
        if (header == null) {
            header = Header.alloc(ctx.alloc());
            event.setHeader(header);
        }

        if (header.isWritable()) {
            ByteBuf in = msg.getBuf();
            in.readBytes(header,
                    Math.min(in.readableBytes(), header.writableBytes()));
            if (header.isWritable()) {
                assert (!msg.getBuf().isReadable());
                msg.getCallback().onSuccess(null);
            } else {
                if (logger.isTraceEnabled()) {
                    String headerHex = BufUtil.hexDump(event.getHeader());
                    logger.trace("Read header 0x{} from {}", headerHex, ctx
                            .channel().remoteAddress());
                }
            }
        }

        // pass on if header is complete
        if (!header.isWritable()) {
            if (event.getBuf() != msg.getBuf()) {
                assert (event.getBuf() == null);
                assert (event.getCallback() == null);
            }
            if (event.getBuf() == null) {
                event.setBuf(msg.getBuf());
            }
            event.setCallback(new Callback(event, msg.getCallback()));
            ctx.nextInboundMessageBuffer().add(event);
        }
    }
}
