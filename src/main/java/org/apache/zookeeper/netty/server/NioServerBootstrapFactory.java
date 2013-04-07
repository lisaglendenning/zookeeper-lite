package org.apache.zookeeper.netty.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;

import java.util.Map;

import com.google.inject.Provider;
import com.google.inject.Provides;

public class NioServerBootstrapFactory extends NioServerModule {

    public static NioServerBootstrapFactory get() {
        return new NioServerBootstrapFactory();
    }

    protected NioServerBootstrapFactory() {
    }

    @Override
    protected void configure() {
        super.configure();
    }

    @Provides
    public ServerBootstrap getServerBootstrap(
            Class<? extends ServerChannel> channelType,
            Provider<EventLoopGroup> eventLoopGroupFactory) {
        return newServerBootstrap(channelType, eventLoopGroupFactory);
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected Map<ChannelOption, Object> getServerChannelOptions() {
        // the same options used in NettyServerCnxnFactory
        Map<ChannelOption, Object> map = super.getServerChannelOptions();
        map.put(ChannelOption.SO_REUSEADDR, Boolean.TRUE);
        return map;
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected Map<ChannelOption, Object> getChannelOptions() {
        // the same options used in NettyServerCnxnFactory
        Map<ChannelOption, Object> map = super.getChannelOptions();
        map.put(ChannelOption.TCP_NODELAY, Boolean.TRUE);
        map.put(ChannelOption.SO_LINGER, Integer.valueOf(2));
        return map;
    }
}