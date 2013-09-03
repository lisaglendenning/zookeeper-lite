package edu.uw.zookeeper.netty.client;

import com.google.common.base.Optional;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import edu.uw.zookeeper.common.EventBusPublisher;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.netty.ChannelClientConnectionFactory;
import edu.uw.zookeeper.netty.ChannelCodecConnection;
import edu.uw.zookeeper.protocol.Codec;

public class NettyClientModule implements NetClientModule {

    public static NettyClientModule newInstance(
            RuntimeModule runtime) {
        Factory<? extends Publisher> publisherFactory = EventBusPublisher.factory();
        Factory<Bootstrap> bootstrapFactory = 
                NioClientBootstrapFactory.newInstance(runtime.threadFactory(), runtime.serviceMonitor());
        return newInstance(publisherFactory, bootstrapFactory);
    }
    
    public static NettyClientModule newInstance(
            Factory<? extends Publisher> publisherFactory,
            Factory<Bootstrap> bootstrapFactory) {
        return new NettyClientModule(publisherFactory, bootstrapFactory);
    }

    protected final Factory<? extends Publisher> publisherFactory;
    protected final Factory<Bootstrap> bootstrapFactory;
    
    public NettyClientModule(
            Factory<? extends Publisher> publisherFactory,
            Factory<Bootstrap> bootstrapFactory) {
        this.publisherFactory = publisherFactory;
        this.bootstrapFactory = bootstrapFactory;
    }
    
    @Override
    public <I, T extends Codec<? super I, ? extends Optional<?>>, C extends Connection<?>> Factory<ChannelClientConnectionFactory<C>> getClientConnectionFactory(
            ParameterizedFactory<Publisher, Pair<Class<I>, T>> codecFactory,
            ParameterizedFactory<Pair<Pair<Class<I>, T>, Connection<I>>, C> connectionFactory) {
        ParameterizedFactory<Channel, C> factory = 
                ChannelCodecConnection.factory(publisherFactory, codecFactory, connectionFactory);
        return ChannelClientConnectionFactory.factory(
                publisherFactory, factory, bootstrapFactory);
    }
}
