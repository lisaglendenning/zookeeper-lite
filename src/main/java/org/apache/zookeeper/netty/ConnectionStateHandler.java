package org.apache.zookeeper.netty;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.ConnectionState;
import org.apache.zookeeper.util.Eventful;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

public class ConnectionStateHandler extends ChannelDuplexHandler {

    public static final String STATE_ATTRIBUTE_NAME = 
            ConnectionState.class.getName();
    public static final AttributeKey<ConnectionState> STATE_ATTRIBUTE_KEY = 
            new AttributeKey<ConnectionState>(STATE_ATTRIBUTE_NAME);

    public static Connection.State getChannelState(Channel channel) {
        Connection.State state = channel.isActive()
                ? Connection.State.CONNECTION_OPENED
                        : (channel.isOpen() 
                                ? Connection.State.CONNECTION_OPENING 
                                        : Connection.State.CONNECTION_CLOSED);
        return state;
    }

    public static ConnectionStateHandler create(Eventful eventful) {
        return new ConnectionStateHandler(eventful);
    }
    
    public static ConnectionStateHandler create(ConnectionState state) {
        return new ConnectionStateHandler(state);
    }

    protected final Logger logger = LoggerFactory.getLogger(ConnectionStateHandler.class);
    protected final ConnectionState state;
    
    @Inject
    public ConnectionStateHandler(Eventful eventful) {
        this(ConnectionState.create(eventful));
    }
    
    public ConnectionStateHandler(ConnectionState state) {
        this.state = state;
    }
    
    public ConnectionState state() {
        return state;
    }
    
    @Override
    public void afterAdd(ChannelHandlerContext ctx) throws Exception {
        Attribute<ConnectionState> attr = ctx.channel().attr(STATE_ATTRIBUTE_KEY);
        attr.compareAndSet(null, state);
        state.set(getChannelState(ctx.channel()));
        super.afterAdd(ctx);
    }
    
    @Override
    public void beforeRemove(ChannelHandlerContext ctx) throws Exception {
        Attribute<ConnectionState> attr = ctx.channel().attr(STATE_ATTRIBUTE_KEY);
        attr.compareAndSet(state, null);
        super.beforeRemove(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.debug("Channel Active {}", ctx.channel().remoteAddress());
        state.compareAndSet(Connection.State.CONNECTION_OPENING, Connection.State.CONNECTION_OPENED);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.debug("Channel Inactive {}", ctx.channel().remoteAddress());
        state.set(Connection.State.CONNECTION_CLOSED);
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        logger.warn("Exception in channel {}", ctx.channel(), cause);
        ctx.close();
        super.exceptionCaught(ctx, cause);
    }
    
    @Override
    public void inboundBufferUpdated(ChannelHandlerContext ctx)
            throws Exception {
        state.compareAndSet(Connection.State.CONNECTION_OPENING, Connection.State.CONNECTION_OPENED);
        ctx.fireInboundBufferUpdated();
    }
    
    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise future) throws Exception {
    	state.set(Connection.State.CONNECTION_CLOSING);
        super.close(ctx, future);
    }

    @Override
    public void flush(ChannelHandlerContext ctx, ChannelPromise promise)
            throws Exception {
        ctx.flush(promise);
    }
}
