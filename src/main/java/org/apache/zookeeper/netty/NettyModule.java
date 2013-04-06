package org.apache.zookeeper.netty;

import java.util.Map;
import java.util.Map.Entry;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;

import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;

public class NettyModule extends AbstractModule {
    @Override
    protected void configure() { 
    }

    protected ChannelGroup newChannelGroup() {
        return new DefaultChannelGroup(getClass().getName());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected Bootstrap newBootstrap(
            Class<? extends Channel> channelType,
            EventLoopGroup group) {
        Bootstrap bootstrap = new Bootstrap()
        .channel(channelType)
            .group(group);

        for (Entry<ChannelOption, Object> entry: getChannelOptions().entrySet()) {
            bootstrap.option(entry.getKey(), entry.getValue());
        }
        
        return bootstrap;
    }

    @SuppressWarnings("rawtypes")
    protected Map<ChannelOption, Object> getChannelOptions() {
        return Maps.newHashMap();
    }
}
