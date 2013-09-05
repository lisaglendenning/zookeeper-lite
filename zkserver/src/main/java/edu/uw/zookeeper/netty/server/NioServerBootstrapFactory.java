package edu.uw.zookeeper.netty.server;

import java.net.SocketAddress;
import java.util.concurrent.ThreadFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import edu.uw.zookeeper.common.DefaultsFactory;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.netty.DaemonThreadFactory;
import edu.uw.zookeeper.netty.EventLoopGroupService;
import edu.uw.zookeeper.netty.SimpleServerBootstrapFactory;
import edu.uw.zookeeper.netty.nio.NioServerChannelTypeFactory;
import edu.uw.zookeeper.netty.nio.NioEventLoopGroupFactory;

public class NioServerBootstrapFactory implements Factory<ServerBootstrap> {

    public static class ParameterizedDecorator implements DefaultsFactory<SocketAddress, ServerBootstrap> {
        public static ParameterizedDecorator newInstance(Factory<ServerBootstrap> delegate) {
            return new ParameterizedDecorator(delegate);
        }
        
        private final Factory<ServerBootstrap> delegate;
        
        private ParameterizedDecorator(Factory<ServerBootstrap> delegate) {
            this.delegate = delegate;
        }

        @Override
        public ServerBootstrap get() {
            return delegate.get();
        }

        @Override
        public ServerBootstrap get(SocketAddress value) {
            return get().localAddress(value);
        }
    }
    
    public static NioServerBootstrapFactory newInstance(
            Factory<ThreadFactory> threadFactory,
            ServiceMonitor serviceMonitor) {
        ThreadFactory threads = DaemonThreadFactory.getInstance().get(threadFactory.get());
        Reference<? extends EventLoopGroup> groupFactory = EventLoopGroupService.factory(
                NioEventLoopGroupFactory.DEFAULT,
                serviceMonitor).get(threads);
        return newInstance(groupFactory);
    }

    public static NioServerBootstrapFactory newInstance(
            Factory<? extends EventLoopGroup> groupFactory) {
        return new NioServerBootstrapFactory(groupFactory);
    }
    
    protected final Factory<? extends EventLoopGroup> groupFactory;
    protected final ParameterizedFactory<Factory<? extends EventLoopGroup>, ServerBootstrap> bootstrapFactory;
    
    protected NioServerBootstrapFactory(
            Factory<? extends EventLoopGroup> groupFactory) {
        this.groupFactory = groupFactory;
        this.bootstrapFactory = SimpleServerBootstrapFactory.newInstance(
                NioServerChannelTypeFactory.getInstance().get(),
                ServerTcpChannelOptionsFactory.getClient().get(),
                ServerTcpChannelOptionsFactory.getServer().get());
    }

    @Override
    public ServerBootstrap get() {
        return bootstrapFactory.get(groupFactory);
    }
}
