package edu.uw.zookeeper.netty.client;

import com.google.common.base.Optional;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.netty.ChannelClientConnectionFactory;
import edu.uw.zookeeper.netty.ChannelConnection;
import edu.uw.zookeeper.protocol.Codec;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class NettyClientModule {

    public static NettyClientModule newInstance(
            RuntimeModule runtime) {
        Factory<Publisher> publisherFactory = runtime.publisherFactory();
        Factory<Bootstrap> bootstrapFactory = 
                NioClientBootstrapFactory.newInstance(runtime.threadFactory(), runtime.serviceMonitor());
        return newInstance(publisherFactory, bootstrapFactory);
    }
    
    public static NettyClientModule newInstance(
            Factory<Publisher> publisherFactory,
            Factory<Bootstrap> bootstrapFactory) {
        return new NettyClientModule(publisherFactory, bootstrapFactory);
    }

    protected final Factory<Publisher> publisherFactory;
    protected final Factory<Bootstrap> bootstrapFactory;
    
    public NettyClientModule(
            Factory<Publisher> publisherFactory,
            Factory<Bootstrap> bootstrapFactory) {
        this.publisherFactory = publisherFactory;
        this.bootstrapFactory = bootstrapFactory;
    }
    
    public <I, O, T extends Codec<I,Optional<O>>, C extends Connection<I>> Factory<ChannelClientConnectionFactory<I,C>> get(
            ParameterizedFactory<Publisher, Pair<Class<I>, T>> codecFactory,
            ParameterizedFactory<Pair<Pair<Class<I>, T>, Connection<I>>, C> connectionFactory) {
        ParameterizedFactory<Channel, C> factory = 
                ChannelConnection.factory(publisherFactory, codecFactory, connectionFactory);
        return ChannelClientConnectionFactory.factory(
                publisherFactory, factory, bootstrapFactory);
    }
}
