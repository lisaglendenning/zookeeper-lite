package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.net.SocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.Provider;

import edu.uw.zookeeper.Connection;
import edu.uw.zookeeper.event.ConnectionEvent;
import edu.uw.zookeeper.event.ConnectionEventValue;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.EventfulBridge;
import edu.uw.zookeeper.util.ForwardingEventful;

public abstract class ChannelConnection extends ForwardingEventful implements
        Connection {

    public static interface Factory<T extends ChannelConnection> {
        public T get(Channel channel);
    }

    protected final Logger logger = LoggerFactory
            .getLogger(ChannelConnection.class);
    protected final EventfulBridge eventfulBridge;
    protected final Channel channel;

    @Inject
    protected ChannelConnection(Channel channel,
            Provider<Eventful> eventfulFactory) {
        super(eventfulFactory.get());
        this.eventfulBridge = new EventfulBridge(eventfulFactory.get(), this);
        this.channel = checkNotNull(channel);
        initChannel();
    }

    public Channel channel() {
        return channel;
    }

    protected void initChannel() {
        DispatchHandler dispatcher = DispatchHandler.create(eventfulBridge);
        channel.pipeline().addLast(DispatchHandler.class.getName(), dispatcher);
        ConnectionStateHandler stateHandler = ConnectionStateHandler
                .create(eventfulBridge);
        channel.pipeline().addBefore(DispatchHandler.class.getName(),
                ConnectionStateHandler.class.getName(), stateHandler);
    }

    @Override
    public State state() {
        ConnectionStateHandler stateHandler = (ConnectionStateHandler) channel
                .pipeline().get(ConnectionStateHandler.class.getName());
        return stateHandler.state().get();
    }

    @Override
    public SocketAddress localAddress() {
        return channel.localAddress();
    }

    @Override
    public SocketAddress remoteAddress() {
        return channel.remoteAddress();
    }

    @Override
    public void read() {
        channel.read();
    }

    @Override
    public <T> ListenableFuture<T> send(T message) {
        ChannelFuture future = channel.write(message);
        ListenableChannelFuture<T> wrapper = ListenableChannelFuture.create(
                future, message);
        return wrapper.promise();
    }

    @Override
    public ListenableFuture<Connection> flush() {
        ChannelFuture future = channel.flush();
        ListenableChannelFuture<Connection> wrapper = ListenableChannelFuture
                .create(future, (Connection) this);
        return wrapper.promise();
    }

    @Override
    public ListenableFuture<Connection> close() {
        logger.debug("Closing {}", this);
        ChannelFuture future = channel.close();
        ListenableChannelFuture<Connection> wrapper = ListenableChannelFuture
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
