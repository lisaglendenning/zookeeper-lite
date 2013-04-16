package edu.uw.zookeeper.netty.server;

import com.google.inject.Provides;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class NioServerModule extends NettyServerModule {

    @Override
    protected void configure() {
        super.configure();
        bind(EventLoopGroup.class).to(NioEventLoopGroup.class);
    }

    @Provides
    public Class<? extends Channel> getChannelType() {
        return NioSocketChannel.class;
    }

    @Provides
    public Class<? extends ServerChannel> getServerChannelType() {
        return NioServerSocketChannel.class;
    }

    @Provides
    public ChannelGroup getChannelGroup() {
        return newChannelGroup();
    }
}
