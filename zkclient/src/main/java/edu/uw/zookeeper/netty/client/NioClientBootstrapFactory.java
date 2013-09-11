package edu.uw.zookeeper.netty.client;

import java.util.concurrent.ThreadFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.netty.DaemonThreadFactory;
import edu.uw.zookeeper.netty.EventLoopGroupService;
import edu.uw.zookeeper.netty.NioEventLoopGroupFactory;
import edu.uw.zookeeper.netty.SimpleBootstrapFactory;

public class NioClientBootstrapFactory implements Factory<Bootstrap> {
    
    public static NioClientBootstrapFactory newInstance(
            Factory<ThreadFactory> threadFactory,
            ServiceMonitor serviceMonitor) {
        ThreadFactory threads = DaemonThreadFactory.getInstance().get(threadFactory.get());
        Reference<? extends EventLoopGroup> groupFactory = EventLoopGroupService.factory(
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
                NioSocketChannel.class,
                ClientTcpChannelOptionsFactory.getInstance());
    }

    @Override
    public Bootstrap get() {
        return bootstrapFactory.get(groupFactory);
    }
}
