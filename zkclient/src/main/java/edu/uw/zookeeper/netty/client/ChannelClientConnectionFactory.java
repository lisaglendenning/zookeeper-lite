package edu.uw.zookeeper.netty.client;

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
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Iterators;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.netty.ChannelConnectionFactory;
import edu.uw.zookeeper.netty.Logging;

public class ChannelClientConnectionFactory<C extends Connection<?>> extends ChannelConnectionFactory<C>
        implements ClientConnectionFactory<C> {
    
    public static <C extends Connection<?>> ClientFactoryBuilder<C> factory(
            Factory<? extends Publisher> publisherFactory,
            ParameterizedFactory<Channel, C> connectionFactory,
            Factory<Bootstrap> bootstrapFactory) {
        return ClientFactoryBuilder.newInstance(
                publisherFactory,
                connectionFactory,
                bootstrapFactory);
    }
    
    public static class ClientFactoryBuilder<C extends Connection<?>> extends FactoryBuilder<C> implements Factory<ChannelClientConnectionFactory<C>> {

        public static <C extends Connection<?>> ClientFactoryBuilder<C> newInstance(
                Factory<? extends Publisher> publisherFactory,
                ParameterizedFactory<Channel, C> connectionFactory,
                Factory<Bootstrap> bootstrapFactory) {
            return new ClientFactoryBuilder<C>(publisherFactory, connectionFactory, bootstrapFactory);
        }
        
        private final Factory<Bootstrap> bootstrapFactory;
        
        private ClientFactoryBuilder(
                Factory<? extends Publisher> publisherFactory,
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
        if (! isRunning()) {
            return Futures.immediateFailedFuture(new IllegalStateException(state().toString()));
        }
        logger.debug(Logging.NETTY_MARKER, "CONNECTING {}", remoteAddress);
        return initializer.connect(bootstrap.connect(remoteAddress));
    }

    @Override
    protected void shutDown() throws Exception {
        initializer.shutDown();
        super.shutDown();
    }

    @ChannelHandler.Sharable
    protected class ChildInitializer extends ChannelInitializer<Channel> {
        
        protected final ConcurrentMap<Channel, C> byChannel;
        protected final Set<ConnectListener> pending;
        
        public ChildInitializer() {
            this.byChannel = new MapMaker().makeMap();
            this.pending = Collections.synchronizedSet(Sets.<ConnectListener>newHashSet());
        }
        
        public ListenableFuture<C> connect(ChannelFuture future) {
            ConnectListener listener = new ConnectListener(future);
            pending.add(listener);
            if (! isRunning()) {
                listener.cancel(true);
            }
            return listener;
        }
    
        // This gets called before connect() completes
        @Override
        public void initChannel(Channel channel) throws Exception {
            C connection = newChannel(channel);
            if (connection != null) {
                byChannel.put(channel, connection);
            }
        }
        
        public void shutDown() {
            synchronized (pending) {
                Iterator<ConnectListener> itr = Iterators.consumingIterator(pending.iterator());
                while (itr.hasNext()) {
                    ConnectListener next = itr.next();
                    if (! next.isDone()) {
                        next.cancel(true);
                    }
                }
            }
            
            Iterator<Map.Entry<Channel, C>> itr = Iterators.consumingIterator(byChannel.entrySet().iterator());
            while (itr.hasNext()) {
                Map.Entry<Channel, C> next = itr.next();
                next.getValue().close();
                next.getKey().close();
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
            public boolean set(C result) {
                boolean set = super.set(result);
                if (set) {
                    pending.remove(this);
                }
                return set;
            }

            @Override
            public boolean setException(Throwable t) {
                boolean setException = super.setException(t);
                if (setException) {
                    pending.remove(this);
                }
                return setException;
            }
            
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                boolean cancel = super.cancel(mayInterruptIfRunning);
                if (cancel) {
                    task().cancel(mayInterruptIfRunning);
                    pending.remove(this);
                }
                return cancel;
            }
        
            // called when connect() completes
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    Channel channel = future.channel();
                    try {
                        C connection = byChannel.remove(channel);
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
}
