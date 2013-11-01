package edu.uw.zookeeper.netty.client;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Codec;
import edu.uw.zookeeper.net.CodecConnection;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.netty.ChannelClientConnectionFactory;
import edu.uw.zookeeper.netty.ChannelCodecConnection;

public class NettyClientModule implements NetClientModule {

    public static NettyClientModule newInstance(
            RuntimeModule runtime) {
        Factory<Bootstrap> bootstrapFactory = 
                NioClientBootstrapFactory.newInstance(runtime.getThreadFactory(), runtime.getServiceMonitor());
        return newInstance(bootstrapFactory);
    }
    
    public static NettyClientModule newInstance(
            Factory<Bootstrap> bootstrapFactory) {
        return new NettyClientModule(bootstrapFactory);
    }

    protected final Factory<Bootstrap> bootstrapFactory;
    
    public NettyClientModule(
            Factory<Bootstrap> bootstrapFactory) {
        this.bootstrapFactory = bootstrapFactory;
    }
    
    @Override
    public <I, O, T extends Codec<I,O,? extends I,? extends O>, C extends Connection<?,?,?>> Factory<? extends ClientConnectionFactory<C>> getClientConnectionFactory(
            Factory<? extends T> codecFactory,
            ParameterizedFactory<CodecConnection<I,O,T,?>, C> connectionFactory) {
        ParameterizedFactory<Channel, C> factory = 
                ChannelCodecConnection.factory(codecFactory, connectionFactory);
        return ChannelClientConnectionFactory.factory(factory, bootstrapFactory);
    }
}
