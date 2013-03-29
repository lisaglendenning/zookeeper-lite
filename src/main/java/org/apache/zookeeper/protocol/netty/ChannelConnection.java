package org.apache.zookeeper.protocol.netty;

import static com.google.common.base.Preconditions.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.net.SocketAddress;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.ConnectionEventValue;
import org.apache.zookeeper.ConnectionStateEvent;
import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.EventfulBridge;
import org.apache.zookeeper.util.ForwardingEventful;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.Provider;

public abstract class ChannelConnection extends ForwardingEventful implements Connection {

    protected final EventfulBridge eventfulBridge;
    protected Channel channel;
    
    @Inject
    protected ChannelConnection(
            Provider<Eventful> eventfulFactory) {
        super(eventfulFactory.get());
        this.eventfulBridge = new EventfulBridge(eventfulFactory.get(), this);
        this.channel = null;
    }

    public Channel channel() {
        return channel;
    }
    
    public void attach(Channel channel) {
        checkNotNull(channel);
        checkState(this.channel == null);
        this.channel = channel;
        DispatchHandler dispatcher = DispatchHandler.create(eventfulBridge);
        channel.pipeline().addLast(DispatchHandler.class.getName(), dispatcher);
        ConnectionStateHandler stateHandler = ConnectionStateHandler.create(eventfulBridge);
        channel.pipeline().addBefore(DispatchHandler.class.getName(), 
                ConnectionStateHandler.class.getName(), stateHandler);
    }

    @Override
    public SocketAddress localAddress() {
        checkState(channel != null);
        return channel.localAddress();
    }
    
    @Override
    public SocketAddress remoteAddress() {
        checkState(channel != null);
        return channel.remoteAddress();
    }

    @Override
    public ListenableFuture<Void> close() {
        checkState(channel != null);
        ChannelFuture future = channel.close();
        ListenableChannelFuture<Void> wrapper = newFuture(future);
        return wrapper.getWrapper();
    }

    @Override
    public ListenableFuture<Void> flush() {
        checkState(channel != null);
        ChannelFuture future = channel.flush();
        ListenableChannelFuture<Void> wrapper = newFuture(future);
        return wrapper.getWrapper();
    }

    @Override
    public ListenableFuture<Void> send(Object message) {
        checkState(channel != null);
        ChannelFuture future = channel.write(message);
        ListenableChannelFuture<Void> wrapper = newFuture(future);
        return wrapper.getWrapper();
    }

    @Override
    public void read() {
        checkState(channel != null);
        channel.read();
    }
    
    @Override
    public State state() {
        if (channel == null) {
            return State.OPENING;
        }
        ConnectionStateHandler stateHandler = (ConnectionStateHandler) channel.pipeline().get(ConnectionStateHandler.class.getName());
        return stateHandler.state().get();
    }
    
    @Override
    public void post(Object event) {
        // TODO: more specific event types?
        if (event instanceof Connection.State) {
            event = ConnectionStateEvent.create(this, (Connection.State)event);
        } else {
            event = ConnectionEventValue.create(this, event);
        }
        super.post(event);
    }

    @Override
    public void register(Object object) {
        super.register(object);
    }

    @Override
    public void unregister(Object object) {
        super.unregister(object);
    }

    protected ListenableChannelFuture<Void> newFuture(ChannelFuture future) {
        return new ListenableChannelFuture<Void>(
                    future,
                    new Function<Channel, Void>() {
                        @Override
                        public Void apply(Channel channel) {
                            return null;
                        }
                    }
                );        
    }
    
}
