package edu.uw.zookeeper.netty;

import com.google.inject.Provides;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class NioModule extends NettyModule {

    @Override
    protected void configure() {
        super.configure();
        bind(EventLoopGroup.class).to(NioEventLoopGroup.class);
        // bind(Channel.class).to(NioSocketChannel.class);
    }

    @Provides
    public Class<? extends Channel> getChannelType() {
        return NioSocketChannel.class;
    }

    @Provides
    public ChannelGroup getChannelGroup() {
        return newChannelGroup();
    }
}
