package edu.uw.zookeeper.netty.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;

import java.util.Map;

import com.google.inject.Provides;

import edu.uw.zookeeper.netty.NioModule;

public class NioBootstrapFactory extends NioModule {

    public static NioBootstrapFactory get() {
        return new NioBootstrapFactory();
    }

    protected NioBootstrapFactory() {
    }

    @Provides
    public Bootstrap getBootstrap(Class<? extends Channel> channelType,
            EventLoopGroup group) {
        return newBootstrap(channelType, group);
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected Map<ChannelOption, Object> getChannelOptions() {
        // the same options used in ClientCnxnSocketNIO
        Map<ChannelOption, Object> map = super.getChannelOptions();
        map.put(ChannelOption.TCP_NODELAY, Boolean.TRUE);
        map.put(ChannelOption.SO_LINGER, Integer.valueOf(-1));
        return map;
    }
}