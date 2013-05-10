package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;

import java.net.SocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.ListenableFuture;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionEvent;
import edu.uw.zookeeper.net.ConnectionEventValue;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ForwardingEventful;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Reference;

public class ChannelConnection extends ForwardingEventful implements
        Connection, Publisher, Reference<Channel> {

    public static class PerConnectionPublisherFactory implements ParameterizedFactory<Channel, ChannelConnection> {

        public static PerConnectionPublisherFactory newInstance(
                Factory<Publisher> publisherFactory) {
            return new PerConnectionPublisherFactory(publisherFactory);
        }
        
        private final Factory<Publisher> publisherFactory;
        
        private PerConnectionPublisherFactory(
                Factory<Publisher> publisherFactory) {
            super();
            this.publisherFactory = publisherFactory;
        }

        @Override
        public ChannelConnection get(Channel channel) {
            Publisher publisher = publisherFactory.get();
            return ChannelConnection.newInstance(publisher, channel);
        }
    }
    
    public static ChannelConnection newInstance(
            Publisher publisher, Channel channel) {
        ChannelConnection connection = new ChannelConnection(publisher, channel);
        initialize(connection);
        return connection;
    }
    
    private static ChannelConnection initialize(ChannelConnection connection) {
        InboundHandler inbound = InboundHandler.newInstance(connection);
        OutboundHandler outbound = OutboundHandler.newInstance();
        ChannelPipeline pipeline = connection.get().pipeline();
        pipeline.addFirst(InboundHandler.class.getName(), inbound);
        pipeline.addLast(OutboundHandler.class.getName(), outbound);
        ConnectionStateHandler stateHandler = ConnectionStateHandler
                .newInstance(connection);
        pipeline.addLast(
                ConnectionStateHandler.class.getName(), stateHandler);
        return connection;
    }
    
    private final Logger logger = LoggerFactory
            .getLogger(ChannelConnection.class);
    private final Channel channel;

    private ChannelConnection(
            Publisher publisher, Channel channel) {
        super(publisher);
        this.channel = checkNotNull(channel);
    }

    @Override
    public Channel get() {
        return channel;
    }

    @Override
    public State state() {
        ConnectionStateHandler stateHandler = get().pipeline().get(ConnectionStateHandler.class);
        return stateHandler.state();
    }

    @Override
    public SocketAddress localAddress() {
        return get().localAddress();
    }

    @Override
    public SocketAddress remoteAddress() {
        return get().remoteAddress();
    }

    @Override
    public ByteBufAllocator allocator() {
        return get().alloc();
    }
    
    @Override
    public void read() {
        get().read();
        // Note that this may result in multiple events for the same buffer
        get().pipeline().fireInboundBufferUpdated();
    }

    @Override
    public ListenableFuture<ByteBuf> write(ByteBuf buf) {
        State state = state();
        switch (state) {
        case CONNECTION_CLOSING:
        case CONNECTION_CLOSED:
            throw new IllegalStateException(state.toString());
        default:
            break;
        }
        ChannelFuture future = get().write(buf);
        ChannelFutureWrapper<ByteBuf> wrapper = ChannelFutureWrapper.create(
                future, buf);
        return wrapper.promise();
    }

    @Override
    public ListenableFuture<Connection> flush() {
        ChannelFuture future = get().flush();
        ChannelFutureWrapper<Connection> wrapper = ChannelFutureWrapper
                .create(future, (Connection) this);
        return wrapper.promise();
    }

    @Override
    public ListenableFuture<Connection> close() {
        logger.debug("Closing: {}", this);
        ChannelFuture future = get().close();
        ChannelFutureWrapper<Connection> wrapper = ChannelFutureWrapper
                .create(future, (Connection) this);
        return wrapper.promise();
    }

    @Override
    public void post(Object event) {
        if (!(event instanceof ConnectionEvent)) {
            event = ConnectionEventValue.create(this, event);
        }
        if (logger.isTraceEnabled()) {
            logger.trace("{}", event);
        }
        super.post(event);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("state", state())
                .add("channel", get()).toString();
    }
}
