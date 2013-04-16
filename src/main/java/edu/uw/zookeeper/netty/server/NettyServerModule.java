package edu.uw.zookeeper.netty.server;

import java.util.Map;
import java.util.Map.Entry;


import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import com.google.common.collect.Maps;
import com.google.inject.Provider;

import edu.uw.zookeeper.netty.NettyModule;

public class NettyServerModule extends NettyModule {

    @Override
    protected void configure() {
        super.configure();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected ServerBootstrap newServerBootstrap(
            Class<? extends ServerChannel> channelType,
            Provider<EventLoopGroup> eventLoopGroupFactory) {
        ServerBootstrap bootstrap = new ServerBootstrap().group(
                eventLoopGroupFactory.get(), eventLoopGroupFactory.get())
                .channel(channelType);

        for (Entry<ChannelOption, Object> entry : getServerChannelOptions()
                .entrySet()) {
            bootstrap.option(entry.getKey(), entry.getValue());
        }

        for (Entry<ChannelOption, Object> entry : getChannelOptions()
                .entrySet()) {
            bootstrap.childOption(entry.getKey(), entry.getValue());
        }

        return bootstrap;
    }

    @SuppressWarnings("rawtypes")
    protected Map<ChannelOption, Object> getServerChannelOptions() {
        return Maps.newHashMap();
    }
}
