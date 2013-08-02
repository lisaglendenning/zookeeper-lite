package edu.uw.zookeeper.netty.nio;

import io.netty.channel.ServerChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import edu.uw.zookeeper.common.Singleton;

public enum NioServerChannelTypeFactory implements Singleton<Class<? extends ServerChannel>> {
    INSTANCE;
    
    public static NioServerChannelTypeFactory getInstance() {
        return INSTANCE;
    }
    
    @Override
    public Class<NioServerSocketChannel> get() {
        return NioServerSocketChannel.class;
    }
}
