package org.apache.zookeeper.client.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Provides;

public class NioBootstrapFactory implements Provider<Bootstrap> {

    public static class BootstrapModule extends AbstractModule {

        public static BootstrapModule get() {
            return new BootstrapModule();
        }
        
        @Override
        protected void configure() {
            bind(Bootstrap.class).toProvider(NioBootstrapFactory.class);
        }

        @Provides
        public BootstrapModule getModule() {
            return this;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        public Bootstrap newBootstrap() {
            Bootstrap bootstrap = new Bootstrap()
            .group(newEventLoopGroup())
            .channel(getChannelType());

            for (Entry<ChannelOption, Object> entry: getChannelOptions().entrySet()) {
                bootstrap.option(entry.getKey(), entry.getValue());
            }
            
            return bootstrap;
        }
        
        @Provides
        public Class<? extends Channel> getChannelType() {
            return NioSocketChannel.class;
        }
        
        @Provides
        public EventLoopGroup newEventLoopGroup() {
            return new NioEventLoopGroup();
        }
        
        @Provides
        public ChannelGroup newChannelGroup() {
            return new DefaultChannelGroup(NioBootstrapFactory.class.getName());
        }
        
        @SuppressWarnings("rawtypes")
        public Map<ChannelOption, Object> getChannelOptions() {
            // the same options used in ClientCnxnSocketNIO
            Map<ChannelOption, Object> map = Maps.newHashMap();
            map.put(ChannelOption.TCP_NODELAY, Boolean.TRUE);
            map.put(ChannelOption.SO_LINGER, Integer.valueOf(-1));
            return map;
        }
    }
    
    protected BootstrapModule module;
    
    @Inject
    protected NioBootstrapFactory(
            BootstrapModule module
            ) {
        this.module = module;
    }

    public Bootstrap get() {
        // TODO: executor/thread factory
        return module.newBootstrap();
    }
}