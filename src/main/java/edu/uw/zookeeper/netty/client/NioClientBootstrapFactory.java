package edu.uw.zookeeper.netty.client;

import java.util.concurrent.ThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import edu.uw.zookeeper.netty.DaemonThreadFactory;
import edu.uw.zookeeper.netty.MonitoredEventLoopGroupFactory;
import edu.uw.zookeeper.netty.SimpleBootstrapFactory;
import edu.uw.zookeeper.netty.nio.NioChannelTypeFactory;
import edu.uw.zookeeper.netty.nio.NioEventLoopGroupFactory;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.ServiceMonitor;
import edu.uw.zookeeper.util.Singleton;

public class NioClientBootstrapFactory implements Factory<Bootstrap> {
    
    public static NioClientBootstrapFactory newInstance(
            Factory<ThreadFactory> threadFactory,
            ServiceMonitor serviceMonitor) {
        ThreadFactory threads = DaemonThreadFactory.getInstance().get(threadFactory.get());
        Singleton<? extends EventLoopGroup> groupFactory = MonitoredEventLoopGroupFactory.newInstance(
                NioEventLoopGroupFactory.DEFAULT,
                serviceMonitor).get(threads);
        return newInstance(groupFactory);
    }

    public static NioClientBootstrapFactory newInstance(
            Factory<? extends EventLoopGroup> groupFactory) {
        return new NioClientBootstrapFactory(groupFactory);
    }
    
    protected final Factory<? extends EventLoopGroup> groupFactory;
    protected final ParameterizedFactory<Factory<? extends EventLoopGroup>, Bootstrap> bootstrapFactory;
    
    protected NioClientBootstrapFactory(Factory<? extends EventLoopGroup> groupFactory) {
        this.groupFactory = groupFactory;
        this.bootstrapFactory = SimpleBootstrapFactory.newInstance(
                NioChannelTypeFactory.getInstance().get(),
                ClientTcpChannelOptionsFactory.getInstance().get());
    }

    @Override
    public Bootstrap get() {
        return bootstrapFactory.get(groupFactory);
    }
}
