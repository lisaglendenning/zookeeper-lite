package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.Publisher;

public class ChannelClientConnectionFactory<C extends Connection<?>> extends ChannelConnectionFactory<C>
        implements ClientConnectionFactory<C> {
    
    public static <C extends Connection<?>> ClientFactoryBuilder<C> factory(
            Factory<Publisher> publisherFactory,
            ParameterizedFactory<Channel, C> connectionFactory,
            Factory<Bootstrap> bootstrapFactory) {
        return ClientFactoryBuilder.newInstance(
                publisherFactory,
                connectionFactory,
                bootstrapFactory);
    }
    
    public static class ClientFactoryBuilder<C extends Connection<?>> extends FactoryBuilder<C> implements Factory<ChannelClientConnectionFactory<C>> {

        public static <C extends Connection<?>> ClientFactoryBuilder<C> newInstance(
                Factory<Publisher> publisherFactory,
                ParameterizedFactory<Channel, C> connectionFactory,
                Factory<Bootstrap> bootstrapFactory) {
            return new ClientFactoryBuilder<C>(publisherFactory, connectionFactory, bootstrapFactory);
        }
        
        private final Factory<Bootstrap> bootstrapFactory;
        
        private ClientFactoryBuilder(
                Factory<Publisher> publisherFactory,
                ParameterizedFactory<Channel, C> connectionFactory,
                Factory<Bootstrap> bootstrapFactory) {
            super(publisherFactory, connectionFactory);
            this.bootstrapFactory = bootstrapFactory;
        }

        @Override
        public ChannelClientConnectionFactory<C> get() {
            return ChannelClientConnectionFactory.newInstance(
                    publisherFactory.get(), 
                    connectionFactory, 
                    bootstrapFactory.get());
        }
    }

    public static <C extends Connection<?>> ChannelClientConnectionFactory<C> newInstance(
            Publisher publisher,
            ParameterizedFactory<Channel, C> connectionFactory,
            Bootstrap bootstrap) {
        ChannelGroup channels = new DefaultChannelGroup(ChannelClientConnectionFactory.class.getSimpleName(), bootstrap.group().next());
        return newInstance(
                publisher, 
                connectionFactory,
                channels,
                bootstrap);
    }
    
    public static <C extends Connection<?>> ChannelClientConnectionFactory<C> newInstance(
            Publisher publisher,
            ParameterizedFactory<Channel, C> connectionFactory,
            ChannelGroup channels,
            Bootstrap bootstrap) {
        return new ChannelClientConnectionFactory<C>(
                publisher, 
                connectionFactory,
                channels,
                bootstrap);
    }

    protected final ChildInitializer initializer;
    protected final Bootstrap bootstrap;

    protected ChannelClientConnectionFactory(
            Publisher publisher,
            ParameterizedFactory<Channel, C> connectionFactory,
            ChannelGroup channels,
            Bootstrap bootstrap) {
        super(publisher, connectionFactory, channels);
        this.initializer = new ChildInitializer();
        this.bootstrap = checkNotNull(bootstrap).handler(initializer);
    }

    @Override
    public ListenableFuture<C> connect(SocketAddress remoteAddress) {
        logger.debug("Connecting to {}", remoteAddress);
        ChannelFuture channelFuture = bootstrap.connect(remoteAddress);
        ConnectListener listener = new ConnectListener(channelFuture);
        return listener;
    }

    @Override
    protected void shutDown() throws Exception {
        // TODO: cancel pending connections?
        initializer.byChannel.clear();
        super.shutDown();
    }

    @ChannelHandler.Sharable
    protected class ChildInitializer extends ChannelInitializer<Channel> {
        
        protected final ConcurrentMap<Channel, C> byChannel;
        
        public ChildInitializer() {
            this.byChannel = Maps.newConcurrentMap();
        }
    
        // This gets called before connect() completes
        @Override
        public void initChannel(Channel channel) throws Exception {
            byChannel.put(channel, newChannel(channel));
        }
    }

    protected class ConnectListener extends PromiseTask<ChannelFuture, C> implements ChannelFutureListener {
    
        public ConnectListener(ChannelFuture channelFuture) {
            this(channelFuture, PromiseTask.<C>newPromise());
        }
        
        public ConnectListener(ChannelFuture channelFuture, Promise<C> promise) {
            super(channelFuture, promise);
            channelFuture.addListener(this);
        }
        
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            task().cancel(mayInterruptIfRunning);
            return super.cancel(mayInterruptIfRunning);
        }
    
        // called when connect() completes
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
                Channel channel = future.channel();
                try {
                    C connection = initializer.byChannel.remove(channel);
                    if (connection != null) {
                        set(connection);
                    } else {
                        cancel(true);
                    }
                } catch (Exception e) {
                    setException(e);
                }
            } else {
                if (future.isCancelled()) {
                    cancel(true);
                } else {
                    setException(future.cause());
                }
            }
        }
    }
}
