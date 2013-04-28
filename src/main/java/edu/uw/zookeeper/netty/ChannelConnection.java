package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.net.SocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.ListenableFuture;
import edu.uw.zookeeper.event.ConnectionEvent;
import edu.uw.zookeeper.event.ConnectionEventValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.util.EventfulAutomaton;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ForwardingEventful;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class ChannelConnection extends ForwardingEventful implements
        Connection, Publisher {

    public static class ConnectionBuilder implements ParameterizedFactory<Channel, ChannelConnection> {

        public static ConnectionBuilder newInstance(
                Factory<Publisher> publisherFactory) {
            return new ConnectionBuilder(publisherFactory);
        }
        
        protected final Factory<Publisher> publisherFactory;
        
        protected ConnectionBuilder(
                Factory<Publisher> publisherFactory) {
            super();
            this.publisherFactory = publisherFactory;
        }

        @Override
        public ChannelConnection get(Channel channel) {
            Publisher publisher = publisherFactory.get();
            return ChannelConnection.create(publisher, channel);
        }
    }
    
    public static ChannelConnection create(
            Publisher publisher, Channel channel) {
        return new ChannelConnection(publisher, channel);
    }
    
    private final Logger logger = LoggerFactory
            .getLogger(ChannelConnection.class);
    private final Channel channel;
    private final ByteBufAllocator allocator;

    private ChannelConnection(
            Publisher publisher, Channel channel) {
        super(publisher);
        this.channel = checkNotNull(channel);
        this.allocator = channel.alloc();

        InboundHandler inbound = InboundHandler.newInstance(this);
        OutboundHandler outbound = OutboundHandler.newInstance();
        channel.pipeline().addFirst(InboundHandler.class.getName(), inbound);
        channel.pipeline().addLast(OutboundHandler.class.getName(), outbound);
        ConnectionStateHandler stateHandler = ConnectionStateHandler
                .create(EventfulAutomaton.createSynchronized(this, Connection.State.class));
        channel.pipeline().addLast(
                ConnectionStateHandler.class.getName(), stateHandler);
    }

    public Channel channel() {
        return channel;
    }

    @Override
    public State state() {
        ConnectionStateHandler stateHandler = channel().pipeline().get(ConnectionStateHandler.class);
        return stateHandler.state();
    }

    @Override
    public SocketAddress localAddress() {
        return channel().localAddress();
    }

    @Override
    public SocketAddress remoteAddress() {
        return channel().remoteAddress();
    }

    @Override
    public ByteBufAllocator allocator() {
        return allocator;
    }
    
    @Override
    public void read() {
        channel().read();
        // Note that this may result in multiple events for the same buffer
        channel().pipeline().fireInboundBufferUpdated();
    }

    @Override
    public ListenableFuture<ByteBuf> write(ByteBuf buf) {
        checkState(state() != State.CONNECTION_CLOSING && state() != State.CONNECTION_CLOSED);
        ChannelFuture future = channel().write(buf);
        ChannelFutureWrapper<ByteBuf> wrapper = ChannelFutureWrapper.create(
                future, buf);
        return wrapper.promise();
    }

    @Override
    public ListenableFuture<Connection> flush() {
        ChannelFuture future = channel().flush();
        ChannelFutureWrapper<Connection> wrapper = ChannelFutureWrapper
                .create(future, (Connection) this);
        return wrapper.promise();
    }

    @Override
    public ListenableFuture<Connection> close() {
        logger.debug("Closing {}", this);
        ChannelFuture future = channel().close();
        ChannelFutureWrapper<Connection> wrapper = ChannelFutureWrapper
                .create(future, (Connection) this);
        return wrapper.promise();
    }

    @Override
    public void post(Object event) {
        if (!(event instanceof ConnectionEvent)) {
            event = ConnectionEventValue.create(this, event);
        }
        super.post(event);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("state", state())
                .add("channel", channel()).toString();
    }
}
