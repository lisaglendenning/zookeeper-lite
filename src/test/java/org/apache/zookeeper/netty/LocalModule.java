package org.apache.zookeeper.netty;

import org.apache.zookeeper.netty.server.NettyServerModule;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.local.LocalServerChannel;

import com.google.inject.Provider;
import com.google.inject.Provides;

public class LocalModule extends NettyServerModule {

    public static LocalModule get() {
        return new LocalModule();
    }

    protected LocalModule() {
    }

    @Override
    protected void configure() {
        super.configure();
        bind(EventLoopGroup.class).to(LocalEventLoopGroup.class);
    }

    @Provides
    public ServerBootstrap getServerBootstrap(
            Class<? extends ServerChannel> channelType,
            Provider<EventLoopGroup> eventLoopGroupFactory) {
        return newServerBootstrap(channelType, eventLoopGroupFactory)
                .localAddress(LocalAddress.ANY);
    }

    @Provides
    public Bootstrap getBootstrap(Class<? extends Channel> channelType,
            EventLoopGroup group) {
        return newBootstrap(channelType, group);
    }

    @Provides
    public Class<? extends Channel> getChannelType() {
        return LocalChannel.class;
    }

    @Provides
    public Class<? extends ServerChannel> getServerChannelType() {
        return LocalServerChannel.class;
    }

    @Provides
    public ChannelGroup getChannelGroup() {
        return newChannelGroup();
    }
}
