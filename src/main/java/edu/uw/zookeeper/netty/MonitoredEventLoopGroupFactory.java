package edu.uw.zookeeper.netty;

import io.netty.channel.EventLoopGroup;

import java.util.concurrent.ThreadFactory;

import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.ServiceMonitor;
import edu.uw.zookeeper.util.Singleton;

public class MonitoredEventLoopGroupFactory implements ParameterizedFactory<ThreadFactory, Factory<? extends EventLoopGroup>> {

    public static MonitoredEventLoopGroupFactory newInstance(
            ParameterizedFactory<ThreadFactory, EventLoopGroup> eventLoopGroupFactory,
            ServiceMonitor serviceMonitor) {
        return new MonitoredEventLoopGroupFactory(eventLoopGroupFactory, serviceMonitor);
    }
    
    protected final ParameterizedFactory<ThreadFactory, EventLoopGroup> eventLoopGroupFactory;
    protected final ServiceMonitor serviceMonitor;
    
    protected MonitoredEventLoopGroupFactory(
            ParameterizedFactory<ThreadFactory, EventLoopGroup> eventLoopGroupFactory,
            ServiceMonitor serviceMonitor) {
        this.eventLoopGroupFactory = eventLoopGroupFactory;
        this.serviceMonitor = serviceMonitor;
    }
    
    @Override
    public Singleton<? extends EventLoopGroup> get(ThreadFactory threadFactory) {
        EventLoopGroup group = eventLoopGroupFactory.get(threadFactory);
        EventLoopGroupService<?> groupService = EventLoopGroupService.newInstance(group);
        serviceMonitor.add(groupService);
        return groupService;
    }
}