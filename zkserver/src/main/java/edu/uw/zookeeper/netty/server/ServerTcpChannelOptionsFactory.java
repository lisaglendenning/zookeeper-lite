package edu.uw.zookeeper.netty.server;

import java.util.Map;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;

import com.google.common.collect.ImmutableMap;

import edu.uw.zookeeper.common.Reference;


public enum ServerTcpChannelOptionsFactory implements Reference<Map<ChannelOption<?>, ?>> {

    // the same options used in NettyServerCnxnFactory
    
    CLIENT(ImmutableMap.<ChannelOption<?>, Object>of(
            ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT,
            ChannelOption.TCP_NODELAY, Boolean.TRUE,
            ChannelOption.SO_LINGER, Integer.valueOf(2))), 
    SERVER(ImmutableMap.<ChannelOption<?>, Object>of(
            ChannelOption.SO_REUSEADDR, Boolean.TRUE));

    public static Map<ChannelOption<?>, ?> getClient() {
        return CLIENT.get();
    }
    
    public static Map<ChannelOption<?>, ?> getServer() {
        return SERVER.get();
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
