package edu.uw.zookeeper.netty.server;

import java.net.SocketAddress;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.Codec;
import edu.uw.zookeeper.net.CodecConnection;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.netty.ChannelCodecConnection;
import edu.uw.zookeeper.netty.ChannelServerConnectionFactory;

public class NettyServerModule implements NetServerModule {

    public static NettyServerModule newInstance(
            RuntimeModule runtime) {
        ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory = 
                NioServerBootstrapFactory.ParameterizedDecorator.newInstance(
                        NioServerBootstrapFactory.newInstance(runtime.getThreadFactory(), runtime.getServiceMonitor()));
        return newInstance(bootstrapFactory);
    }

    public static NettyServerModule newInstance(
            ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory) {
        return new NettyServerModule(bootstrapFactory);
    }
    
    protected final ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory;
    
    public NettyServerModule(
            ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory) {
        this.bootstrapFactory = bootstrapFactory;
    }
    
    @Override
    public <I, O, T extends Codec<I,O,? extends I,? extends O>, C extends Connection<?,?,?>> ParameterizedFactory<SocketAddress, ? extends ServerConnectionFactory<C>> getServerConnectionFactory(
            Factory<? extends T> codecFactory,
            ParameterizedFactory<CodecConnection<I,O,T,?>, C> connectionFactory) {
        ParameterizedFactory<Channel, C> factory = 
                ChannelCodecConnection.factory(codecFactory, connectionFactory);
        return ChannelServerConnectionFactory.factory(factory, bootstrapFactory);
    }
}
