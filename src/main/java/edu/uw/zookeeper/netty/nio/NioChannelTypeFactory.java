package edu.uw.zookeeper.netty.nio;

import io.netty.channel.Channel;
import io.netty.channel.socket.nio.NioSocketChannel;
import edu.uw.zookeeper.common.Singleton;

public enum NioChannelTypeFactory implements Singleton<Class<? extends Channel>> {
    INSTANCE;
    
    public static NioChannelTypeFactory getInstance() {
        return INSTANCE;
    }
    
    @Override
    public Class<NioSocketChannel> get() {
        return NioSocketChannel.class;
    }
}
