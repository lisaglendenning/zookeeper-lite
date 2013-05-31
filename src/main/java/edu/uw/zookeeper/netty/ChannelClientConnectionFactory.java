package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;

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

public class ChannelClientConnectionFactory<I, C extends Connection<I>> extends ChannelConnectionFactory<I,C>
        implements ClientConnectionFactory<I,C> {
    
    public static <I, C extends Connection<I>> ClientFactoryBuilder<I,C> factory(
            final Factory<Publisher> publisherFactory,
            ParameterizedFactory<Channel, C> connectionFactory,
            Factory<Bootstrap> bootstrapFactory) {
        return ClientFactoryBuilder.newInstance(
                publisherFactory,
                connectionFactory,
                bootstrapFactory);
    }
    
    public static class ClientFactoryBuilder<I, C extends Connection<I>> extends FactoryBuilder<I,C> implements Factory<ChannelClientConnectionFactory<I,C>> {

        public static <I, C extends Connection<I>> ClientFactoryBuilder<I,C> newInstance(
                Factory<Publisher> publisherFactory,
                ParameterizedFactory<Channel, C> connectionFactory,
                Factory<Bootstrap> bootstrapFactory) {
            return new ClientFactoryBuilder<I,C>(publisherFactory, connectionFactory, bootstrapFactory);
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
        public ChannelClientConnectionFactory<I,C> get() {
            return ChannelClientConnectionFactory.newInstance(
                    publisherFactory.get(), 
                    connectionFactory, 
                    bootstrapFactory.get());
        }
    }
    
    public static <I, C extends Connection<I>> ChannelClientConnectionFactory<I,C> newInstance(
            Publisher publisher,
            ParameterizedFactory<Channel, C> connectionFactory,
            Bootstrap bootstrap) {
        return new ChannelClientConnectionFactory<I,C>(
                publisher, 
                connectionFactory,
                ChannelGroupFactory.getInstance().get(ChannelClientConnectionFactory.class.getSimpleName()),
                bootstrap);
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
                    C connection = initializer().byChannel.remove(channel);
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

    private final ChildInitializer initializer;
    private final Bootstrap bootstrap;

    private ChannelClientConnectionFactory(
            Publisher publisher,
            ParameterizedFactory<Channel, C> connectionFactory,
            ChannelGroup group,
            Bootstrap bootstrap) {
        super(publisher, connectionFactory, group);
        this.initializer = new ChildInitializer();
        this.bootstrap = checkNotNull(bootstrap).handler(initializer);
    }
    
    protected ChildInitializer initializer() {
        return initializer;
    }

    protected Bootstrap bootstrap() {
        return bootstrap;
    }

    @Override
    public ListenableFuture<C> connect(SocketAddress remoteAddress) {
        logger().debug("Connecting to {}", remoteAddress);
        ChannelFuture channelFuture = bootstrap().connect(remoteAddress);
        ConnectListener listener = new ConnectListener(channelFuture);
        return listener;
    }

    @Override
    protected void shutDown() throws Exception {
        // TODO: cancel pending connections?
        initializer().byChannel.clear();
        super.shutDown();
    }
}
