package edu.uw.zookeeper.netty.client;

import java.util.Map;

import io.netty.channel.ChannelOption;
import com.google.common.collect.ImmutableMap;

import edu.uw.zookeeper.common.Singleton;


public enum ClientTcpChannelOptionsFactory implements Singleton<Map<ChannelOption<?>, ?>> {

    // the same options used in ClientCnxnSocketNIO
    INSTANCE(ImmutableMap.<ChannelOption<?>, Object>of(
                    ChannelOption.TCP_NODELAY, Boolean.TRUE,
                    ChannelOption.SO_LINGER, Integer.valueOf(-1)));

    public static ClientTcpChannelOptionsFactory getInstance() {
        return INSTANCE;
    }
    
    private final ImmutableMap<ChannelOption<?>, ?> options;

    private ClientTcpChannelOptionsFactory(ImmutableMap<ChannelOption<?>, ?> options) {
        this.options = options;
    }
    
    @Override
    public Map<ChannelOption<?>, ?> get() {
        return options;
    }
}