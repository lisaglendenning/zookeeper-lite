package edu.uw.zookeeper.netty.server;

import java.util.Map;

import io.netty.channel.ChannelOption;
import com.google.common.collect.ImmutableMap;

import edu.uw.zookeeper.util.Singleton;


public enum ServerTcpChannelOptionsFactory implements Singleton<Map<ChannelOption<?>, ?>> {

    // the same options used in NettyServerCnxnFactory
    
    CLIENT(ImmutableMap.<ChannelOption<?>, Object>of(
            ChannelOption.TCP_NODELAY, Boolean.TRUE,
            ChannelOption.SO_LINGER, Integer.valueOf(2))), 
    SERVER(ImmutableMap.<ChannelOption<?>, Object>of(
            ChannelOption.SO_REUSEADDR, Boolean.TRUE));

    public static ServerTcpChannelOptionsFactory getClient() {
        return CLIENT;
    }
    
    public static ServerTcpChannelOptionsFactory getServer() {
        return SERVER;
    }
    
    private final ImmutableMap<ChannelOption<?>, ?> options;

    private ServerTcpChannelOptionsFactory(ImmutableMap<ChannelOption<?>, ?> options) {
        this.options = options;
    }
    
    @Override
    public Map<ChannelOption<?>, ?> get() {
        return options;
    }
}
