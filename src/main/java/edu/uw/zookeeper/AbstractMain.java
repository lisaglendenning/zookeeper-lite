package edu.uw.zookeeper;

import java.util.concurrent.ThreadFactory;

import edu.uw.zookeeper.Factories.PublisherFactory;
import edu.uw.zookeeper.util.Application;
import edu.uw.zookeeper.util.ConfigurableMain;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.HolderFactory;
import edu.uw.zookeeper.util.ListeningExecutorServiceFactory;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.ServiceMonitor;
import edu.uw.zookeeper.util.Singleton;


public abstract class AbstractMain implements Runnable {
    
    public static void main(String[] args) {
        ConfigurableMain.main(args, ConfigurableMain.DefaultApplicationFactory.newInstance(AbstractMain.class));
    }
    
    protected final Factory<Publisher> publisherFactory;
    protected final Singleton<ServiceMonitor> serviceMonitor;
    protected final Singleton<Configuration> configuration;
    protected final ListeningExecutorServiceFactory executors;
    
    protected AbstractMain(Configuration configuration) {
        this.configuration = HolderFactory.newInstance(configuration);
        this.publisherFactory = PublisherFactory.getInstance();
        this.serviceMonitor = HolderFactory.newInstance(ServiceMonitor.newInstance());
        this.executors = Factories.listeningExecutors(serviceMonitor.get());
    }
    
    public Configuration configuration() {
        return configuration.get();
    }
    
    public Factory<ThreadFactory> threadFactory() {
        return Factories.PlatformThreadFactory.getInstance();
    }
    
    public ServiceMonitor serviceMonitor() {
        return serviceMonitor.get();
    }
    
    public Factory<Publisher> publisherFactory() {
        return publisherFactory;
    }
    
    public ListeningExecutorServiceFactory executors() {
        return executors;
    }
    
    @Override
    public void run() {
        Application application = application();
        ConfigurableMain.exitIfHelpSet(configuration().asArguments());
        application.run();
    }

    protected abstract Application application();
}
