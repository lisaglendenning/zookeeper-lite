package edu.uw.zookeeper.netty.client;

import java.util.Map;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;

import com.google.common.collect.ImmutableMap;

import edu.uw.zookeeper.common.Reference;


public enum ClientTcpChannelOptionsFactory implements Reference<Map<ChannelOption<?>, ?>> {

    // the same options used in ClientCnxnSocketNIO
    INSTANCE(ImmutableMap.<ChannelOption<?>, Object>of(
                    ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT,
                    ChannelOption.TCP_NODELAY, Boolean.TRUE,
                    ChannelOption.SO_LINGER, Integer.valueOf(-1)));

    public static Map<ChannelOption<?>, ?> getInstance() {
        return INSTANCE.get();
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