package edu.uw.zookeeper.common;

import java.util.concurrent.ThreadFactory;

public interface RuntimeModule {

    Configuration getConfiguration();
    
    Factory<ThreadFactory> getThreadFactory();
    
    ServiceMonitor getServiceMonitor();
    
    ListeningExecutorServiceFactory getExecutors();

    void shutdown();
}
