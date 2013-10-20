package edu.uw.zookeeper.netty.client;

import net.engio.mbassy.PubSubSupport;
import net.engio.mbassy.bus.SyncBusConfiguration;
import net.engio.mbassy.bus.SyncMessageBus;

import com.google.common.base.Optional;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.netty.ChannelCodecConnection;
import edu.uw.zookeeper.protocol.Codec;

public class NettyClientModule implements NetClientModule {

    public static NettyClientModule newInstance(
            RuntimeModule runtime) {
        Factory<Bootstrap> bootstrapFactory = 
                NioClientBootstrapFactory.newInstance(runtime.getThreadFactory(), runtime.getServiceMonitor());
        return newInstance(syncMessageBus(), bootstrapFactory);
    }
    
    public static NettyClientModule newInstance(
            Factory<? extends PubSubSupport<Object>> publisherFactory,
            Factory<Bootstrap> bootstrapFactory) {
        return new NettyClientModule(publisherFactory, bootstrapFactory);
    }

    public static Factory<SyncMessageBus<Object>> syncMessageBus() {
        return new Factory<SyncMessageBus<Object>>() {
            @SuppressWarnings("rawtypes")
            @Override
            public SyncMessageBus<Object> get() {
                return new SyncMessageBus<Object>(new SyncBusConfiguration());
            }
        };
    }

    protected final Factory<? extends PubSubSupport<Object>> publisherFactory;
    protected final Factory<Bootstrap> bootstrapFactory;
    
    public NettyClientModule(
            Factory<? extends PubSubSupport<Object>> publisherFactory,
            Factory<Bootstrap> bootstrapFactory) {
        this.publisherFactory = publisherFactory;
        this.bootstrapFactory = bootstrapFactory;
    }
    
    @Override
    public <I, T extends Codec<? super I, ? extends Optional<?>>, C extends Connection<?>> Factory<ChannelClientConnectionFactory<C>> getClientConnectionFactory(
            ParameterizedFactory<PubSubSupport<Object>, ? extends Pair<Class<I>, ? extends T>> codecFactory,
            ParameterizedFactory<Pair<? extends Pair<Class<I>, ? extends T>, Connection<I>>, C> connectionFactory) {
        ParameterizedFactory<Channel, C> factory = 
                ChannelCodecConnection.factory(publisherFactory, codecFactory, connectionFactory);
        return ChannelClientConnectionFactory.factory(
                publisherFactory, factory, bootstrapFactory);
    }
}
