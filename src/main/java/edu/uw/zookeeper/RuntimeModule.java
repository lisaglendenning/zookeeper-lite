package edu.uw.zookeeper;

import java.util.concurrent.ThreadFactory;

import edu.uw.zookeeper.AbstractMain.ListeningExecutorServiceFactory;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.ServiceMonitor;

public interface RuntimeModule {

    Configuration configuration();
    
    Factory<ThreadFactory> threadFactory();
    
    ServiceMonitor serviceMonitor();
    
    Factory<Publisher> publisherFactory();
    
    ListeningExecutorServiceFactory executors();
    
}
