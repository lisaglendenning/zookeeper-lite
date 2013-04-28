package edu.uw.zookeeper.netty.server;

import java.util.concurrent.ThreadFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import edu.uw.zookeeper.netty.DaemonThreadFactory;
import edu.uw.zookeeper.netty.MonitoredEventLoopGroupFactory;
import edu.uw.zookeeper.netty.SimpleServerBootstrapFactory;
import edu.uw.zookeeper.netty.nio.NioServerChannelTypeFactory;
import edu.uw.zookeeper.netty.nio.NioEventLoopGroupFactory;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.ServiceMonitor;
import edu.uw.zookeeper.util.Singleton;

public class NioServerBootstrapFactory implements Factory<ServerBootstrap> {

    public static NioServerBootstrapFactory newInstance(
            Factory<ThreadFactory> threadFactory,
            ServiceMonitor serviceMonitor) {
        ThreadFactory threads = DaemonThreadFactory.getInstance().get(threadFactory.get());
        Singleton<? extends EventLoopGroup> groupFactory = MonitoredEventLoopGroupFactory.newInstance(
                NioEventLoopGroupFactory.getInstance(),
                serviceMonitor).get(threads);
        return newInstance(groupFactory);
    }

    public static NioServerBootstrapFactory newInstance(Factory<? extends EventLoopGroup> groupFactory) {
        return new NioServerBootstrapFactory(groupFactory);
    }
    
    protected final Factory<? extends EventLoopGroup> groupFactory;
    protected final ParameterizedFactory<Factory<? extends EventLoopGroup>, ServerBootstrap> bootstrapFactory;
    
    protected NioServerBootstrapFactory(Factory<? extends EventLoopGroup> groupFactory) {
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
