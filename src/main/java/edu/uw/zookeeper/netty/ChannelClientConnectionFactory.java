package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.group.ChannelGroup;

import java.net.SocketAddress;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class ChannelClientConnectionFactory extends ChannelConnectionFactory
        implements ClientConnectionFactory {

    public static enum ChannelGroupFactory implements DefaultsFactory<String, ChannelGroup> {
        INSTANCE;
        
        public static Factory<ChannelGroup> getInstance() {
            return INSTANCE;
        }
        
        @Override
        public ChannelGroup get() {
            return ChannelConnectionFactory.newChannelGroup(ChannelClientConnectionFactory.class.getSimpleName());
        }

        @Override
        public ChannelGroup get(String name) {
            return ChannelConnectionFactory.newChannelGroup(name);
        }
    }
    
    public static class ClientFactoryBuilder extends FactoryBuilder implements Factory<ChannelClientConnectionFactory> {

        public static Factory<ChannelClientConnectionFactory> newInstance(
                Factory<Publisher> publisherFactory,
                ParameterizedFactory<Channel, ChannelConnection> connectionFactory,
                Factory<Bootstrap> bootstrapFactory) {
            return new ClientFactoryBuilder(publisherFactory, connectionFactory, bootstrapFactory);
        }
        
        private final Factory<Bootstrap> bootstrapFactory;
        
        private ClientFactoryBuilder(
                Factory<Publisher> publisherFactory,
                ParameterizedFactory<Channel, ChannelConnection> connectionFactory,
                Factory<Bootstrap> bootstrapFactory) {
            super(publisherFactory, connectionFactory);
            this.bootstrapFactory = bootstrapFactory;
        }

        @Override
        public ChannelClientConnectionFactory get() {
            return ChannelClientConnectionFactory.newInstance(
                    publisherFactory.get(), 
                    connectionFactory, 
                    bootstrapFactory.get());
        }
    }
    
    public static ChannelClientConnectionFactory newInstance(
            Publisher publisher,
            ParameterizedFactory<Channel, ChannelConnection> connectionFactory,
            Bootstrap bootstrap) {
        return new ChannelClientConnectionFactory(publisher, connectionFactory,
                bootstrap);
    }

    protected class ConnectListener implements ChannelFutureListener {
        protected SettableFuture<Connection> promise;

        public ConnectListener(SettableFuture<Connection> promise) {
            this.promise = promise;
        }

        // called when connect() completes
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
                try {
                    ChannelConnection connection = ChannelClientConnectionFactory.super
                        .add(future.channel());
                    promise.set(connection);
                } catch (Exception e) {
                    promise.setException(e);
                }
            } else {
                if (future.isCancelled()) {
                    promise.cancel(true);
                } else {
                    promise.setException(future.cause());
                }
            }
        }
    }

    private final Bootstrap bootstrap;

    private ChannelClientConnectionFactory(
            Publisher publisher,
            ParameterizedFactory<Channel, ChannelConnection> connectionFactory,
            Bootstrap bootstrap) {
        super(publisher, connectionFactory, ChannelGroupFactory.getInstance().get());
        this.bootstrap = checkNotNull(bootstrap).handler(new ChildInitializer());
    }

    protected Bootstrap bootstrap() {
        return bootstrap;
    }

    @Override
    public ListenableFuture<Connection> connect(SocketAddress remoteAddress) {
        logger().debug("Connecting to {}", remoteAddress);
        SettableFuture<Connection> future = SettableFuture.create();
        ChannelFuture channelFuture = bootstrap().connect(remoteAddress);
        channelFuture.addListener(new ConnectListener(future));
        return future;
    }

    @Override
    protected ChannelConnection add(Channel channel) {
        return null;
    }
    /*
    @Override
    protected void shutDown() throws Exception {
        // TODO: cancel pending connections?
        super.shutDown();
        // deprecated bootstrap.shutdown();
    }*/
}
