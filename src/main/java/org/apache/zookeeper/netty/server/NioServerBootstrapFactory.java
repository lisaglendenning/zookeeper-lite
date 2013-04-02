package org.apache.zookeeper.netty.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.zookeeper.client.Client;
import org.apache.zookeeper.util.Arguments;
import org.apache.zookeeper.util.Configurable;
import org.apache.zookeeper.util.ConfigurableSocketAddress;
import org.apache.zookeeper.util.Configuration;
import org.apache.zookeeper.util.Parameters;

import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Provides;

public class NioServerBootstrapFactory implements Provider<ServerBootstrap>, Configurable {

    public static final String ARG_ADDRESS = "address";
    public static final String ARG_PORT = "port";
    
    public static final String PARAM_KEY_ADDRESS = "Server.Address";
    public static final String PARAM_DEFAULT_ADDRESS = "";
    
    public static final String PARAM_KEY_PORT = "Server.Port";
    public static final int PARAM_DEFAULT_PORT = 2181;
    
    public static class ServerBootstrapModule extends AbstractModule {

        public static ServerBootstrapModule get() {
            return new ServerBootstrapModule();
        }
        
        public ServerBootstrapModule() {}
        
        @Override
        protected void configure() {
            bind(ServerBootstrap.class).toProvider(NioServerBootstrapFactory.class);
        }

        @Provides
        public ServerBootstrapModule getModule() {
            return this;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        public ServerBootstrap newServerBootstrap() {
            ServerBootstrap bootstrap = new ServerBootstrap()
            .group(newEventLoopGroup(), newEventLoopGroup())
            .channel(getServerChannelType());
    
            for (Entry<ChannelOption, Object> entry: getServerChannelOptions().entrySet()) {
                bootstrap.option(entry.getKey(), entry.getValue());
            }
        
            for (Entry<ChannelOption, Object> entry: getChildChannelOptions().entrySet()) {
                bootstrap.childOption(entry.getKey(), entry.getValue());
            }
            
            return bootstrap;
        }
        
        @Provides
        public Class<? extends ServerChannel> getServerChannelType() {
            return NioServerSocketChannel.class;
        }
        
        @Provides
        public EventLoopGroup newEventLoopGroup() {
            return new NioEventLoopGroup();
        }
        
        @Provides
        public ChannelGroup newChannelGroup() {
            return new DefaultChannelGroup(NioServerBootstrapFactory.class.getName());
        }
        
        @SuppressWarnings("rawtypes")
        public Map<ChannelOption, Object> getServerChannelOptions() {
            // the same options used in NettyServerCnxnFactory
            Map<ChannelOption, Object> map = Maps.newHashMap();
            map.put(ChannelOption.SO_REUSEADDR, Boolean.TRUE);
            return map;
        }

        @SuppressWarnings("rawtypes")
        public Map<ChannelOption, Object> getChildChannelOptions() {
            // the same options used in NettyServerCnxnFactory
            Map<ChannelOption, Object> map = Maps.newHashMap();
            map.put(ChannelOption.TCP_NODELAY, Boolean.TRUE);
            map.put(ChannelOption.SO_LINGER, Integer.valueOf(2));
            return map;
        }
    }
    
    protected final ConfigurableSocketAddress address;
    protected final ServerBootstrapModule module;
    
    @Inject
    protected NioServerBootstrapFactory(
            Arguments arguments,
            Configuration configuration,
            ServerBootstrapModule module
            ) throws Exception {
        arguments.add(arguments.newOption(ARG_ADDRESS, "ServerAddress"))
            .add(arguments.newOption(ARG_PORT, "ServerPort"));
        arguments.parse();
        if (arguments.hasValue(ARG_ADDRESS)) {
            configuration.set(PARAM_KEY_ADDRESS, arguments.getValue(ARG_ADDRESS));
        }
        if (arguments.hasValue(ARG_PORT)) {
            configuration.set(PARAM_KEY_PORT, Integer.valueOf(arguments.getValue(ARG_PORT)));
        }
        this.address = ConfigurableSocketAddress.create(PARAM_KEY_ADDRESS,
                PARAM_DEFAULT_ADDRESS, PARAM_KEY_PORT, PARAM_DEFAULT_PORT);
        this.module = module;
        configure(configuration);
    }

    @Override
    public void configure(Configuration configuration) {
        address.configure(configuration);
    }

    public ServerBootstrap get() {
        // TODO: executor/thread factory
        ServerBootstrap bootstrap = module.newServerBootstrap()
            .localAddress(address.socketAddress());
        // increment port (not thread safe!)
        address.setPort(address.port() + 1);
        return bootstrap;
    }
}