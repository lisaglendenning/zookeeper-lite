package edu.uw.zookeeper.netty.server;

import java.util.Map;
import java.util.Map.Entry;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;

// TODO: refactor overlap with SimpleBootstrapFactory
public class SimpleServerBootstrapFactory implements ParameterizedFactory<Factory<? extends EventLoopGroup>, ServerBootstrap> {

    public static SimpleServerBootstrapFactory newInstance(
            Class<? extends ServerChannel> serverChannelType,
            Map<ChannelOption<?>, ?> channelOptions,
            Map<ChannelOption<?>, ?> serverChannelOptions) {
        return new SimpleServerBootstrapFactory(serverChannelType, channelOptions, serverChannelOptions);
    }
    
    private final Class<? extends ServerChannel> serverChannelType;
    private final Map<ChannelOption<?>, ?> channelOptions;
    private final Map<ChannelOption<?>, ?> serverChannelOptions;
    
    @SuppressWarnings("unchecked")
    public static <T> ServerBootstrap option(ServerBootstrap bootstrap, ChannelOption<T> key, Object value) {
        return bootstrap.option(key, (T) value);
    }

    @SuppressWarnings("unchecked")
    public static <T> ServerBootstrap childOption(ServerBootstrap bootstrap, ChannelOption<T> key, Object value) {
        return bootstrap.childOption(key, (T) value);
    }
    
    protected SimpleServerBootstrapFactory(
            Class<? extends ServerChannel> serverChannelType,
            Map<ChannelOption<?>, ?> channelOptions,
            Map<ChannelOption<?>, ?> serverChannelOptions) {
        this.serverChannelType = serverChannelType;
        this.channelOptions = channelOptions;
        this.serverChannelOptions = serverChannelOptions;
    }
    
    @Override
    public ServerBootstrap get(Factory<? extends EventLoopGroup> eventLoopGroupFactory) {
        ServerBootstrap bootstrap = new ServerBootstrap()
            .channel(serverChannelType)
            .group(eventLoopGroupFactory.get(), eventLoopGroupFactory.get());

        for (Entry<ChannelOption<?>, ? extends Object> entry : serverChannelOptions.entrySet()) {
            option(bootstrap, entry.getKey(), entry.getValue());
        }

        for (Entry<ChannelOption<?>, ? extends Object> entry : channelOptions.entrySet()) {
            childOption(bootstrap, entry.getKey(), entry.getValue());
        }

        return bootstrap;
    }
}
