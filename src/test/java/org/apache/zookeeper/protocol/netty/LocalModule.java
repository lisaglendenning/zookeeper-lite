package org.apache.zookeeper.protocol.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.local.LocalServerChannel;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class LocalModule extends AbstractModule {

    public static LocalModule get() {
        return new LocalModule();
    }
    
    protected LocalModule() {}

    @Provides
    public Bootstrap clientBootstrap() {
        Bootstrap bootstrap = new Bootstrap()
            .group(newEventLoopGroup())
            .channel(getChannelType());
        return bootstrap;
    }

    @Provides
    public ServerBootstrap serverBootstrap() {
        ServerBootstrap bootstrap = new ServerBootstrap()
            .localAddress(LocalAddress.ANY)
            .group(newEventLoopGroup())
            .channel(getServerChannelType());
        return bootstrap;
    }

    @Override
    protected void configure() {
    }

    @Provides
    protected Class<? extends Channel> getChannelType() {
        return LocalChannel.class;
    }

    @Provides
    protected Class<? extends ServerChannel> getServerChannelType() {
        return LocalServerChannel.class;
    }
    
    @Provides
    protected EventLoopGroup newEventLoopGroup() {
        return new LocalEventLoopGroup();
    }
    
    @Provides
    protected ChannelGroup newChannelGroup() {
        return new DefaultChannelGroup(getClass().getSimpleName());
    }
}
