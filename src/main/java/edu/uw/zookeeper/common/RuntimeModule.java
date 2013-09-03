package edu.uw.zookeeper.common;

import java.util.concurrent.ThreadFactory;

public interface RuntimeModule {

    Configuration configuration();
    
    Factory<ThreadFactory> threadFactory();
    
    ServiceMonitor serviceMonitor();
    
    ListeningExecutorServiceFactory executors();

    void shutdown();
}
