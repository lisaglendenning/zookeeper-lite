package edu.uw.zookeeper;

import java.util.concurrent.ThreadFactory;

import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ServiceMonitor;

public interface RuntimeModule {

    Configuration configuration();
    
    Factory<ThreadFactory> threadFactory();
    
    ServiceMonitor serviceMonitor();
    
    Factory<? extends Publisher> publisherFactory();
    
    ListeningExecutorServiceFactory executors();
}
