package edu.uw.zookeeper;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;

import edu.uw.zookeeper.common.Arguments;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.DefaultsFactory;
import edu.uw.zookeeper.common.EventBusPublisher;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.SimpleArguments;
import edu.uw.zookeeper.common.TimeValue;


public class DefaultRuntimeModule implements RuntimeModule {

    public static DefaultRuntimeModule newInstance() {
        return new DefaultRuntimeModule();
    }
    
    public static enum DefaultArgumentsFactory implements DefaultsFactory<String[], Arguments> {
        INSTANCE;
        
        public static DefaultArgumentsFactory getInstance() {
            return INSTANCE;
        }
        
        @Override
        public SimpleArguments get() {
            return get(new String[0]);
        }

        @Override
        public SimpleArguments get(String[] value) {
            SimpleArguments arguments = SimpleArguments.create();
            arguments.setArgs(value);
            return arguments;
        }
    }
    
    public static enum DefaultConfigurationFactory implements ParameterizedFactory<Arguments, Configuration> {
        INSTANCE;
        
        public static DefaultConfigurationFactory getInstance() {
            return INSTANCE;
        }
        
        private final Factory<Config> configFactory = Configuration.DefaultConfigFactory.Holder.getInstance();
        
        @Override
        public Configuration get(Arguments arguments) {
            Config defaultConfig = configFactory.get();
            return Configuration.create(arguments, defaultConfig);
        }
    }
    
    public static enum EventBusPublisherFactory implements Factory<EventBusPublisher> {
        INSTANCE;

        public static EventBusPublisherFactory getInstance() {
            return INSTANCE;
        }

        @Override
        public EventBusPublisher get() {
            return new EventBusPublisher();
        }        
    }
    
    public static enum PlatformThreadFactory implements Factory<ThreadFactory> {
        INSTANCE;

        public static PlatformThreadFactory getInstance() {
            return INSTANCE;
        }
        
        @Override
        public ThreadFactory get() {
            return MoreExecutors.platformThreadFactory();
        }
    }
    
    public static enum SingleDaemonThreadScheduledExectorFactory implements DefaultsFactory<ThreadFactory, ScheduledExecutorService> {
        INSTANCE;

        public static SingleDaemonThreadScheduledExectorFactory getInstance() {
            return INSTANCE;
        }
        
        private final String nameFormat = "scheduled-%d";

        @Override
        public ScheduledExecutorService get() {
            return get(PlatformThreadFactory.getInstance().get());
        }
        
        @Override
        public ScheduledExecutorService get(ThreadFactory threadFactory) {
            ScheduledExecutorService instance = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                    .setThreadFactory(threadFactory)
                    .setDaemon(true)
                    .setNameFormat(nameFormat)
                    .build());
            //MoreExecutors.addDelayedShutdownHook(instance, 100, TimeUnit.MILLISECONDS);
            return instance;
        }
    }
    
    public static enum ApplicationExecutorFactory implements DefaultsFactory<ThreadFactory, ExecutorService> {
        INSTANCE;

        public static ApplicationExecutorFactory getInstance() {
            return INSTANCE;
        }
        
        // TODO: configurable
        private final int CORE_SIZE = Math.max(1,
                Runtime.getRuntime().availableProcessors());
        private final String nameFormat = "main-pool-%d";

        @Override
        public ExecutorService get() {
            return get(PlatformThreadFactory.getInstance().get());
        }
        
        public ExecutorService get(ThreadFactory threadFactory) {
            int corePoolSize = CORE_SIZE;
            int maxPoolSize = CORE_SIZE;
            TimeValue keepAlive = TimeValue.create(60L, TimeUnit.SECONDS);
            BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
            ThreadFactory threads = new ThreadFactoryBuilder()
                .setThreadFactory(threadFactory)
                .setNameFormat(nameFormat)
                .build();
            ThreadPoolExecutor executor = new ThreadPoolExecutor(
                    corePoolSize, maxPoolSize,
                    keepAlive.value(), keepAlive.unit(),
                    queue,
                    threads);
            executor.prestartAllCoreThreads();
            return executor;
        }
    }
    
    public static ListeningExecutorServiceFactory listeningExecutors(ServiceMonitor serviceMonitor) {
        return ListeningExecutorServiceFactory.newInstance(
                serviceMonitor,
                ImmutableMap.<Class<? extends ExecutorService>, Factory<? extends ExecutorService>>of(
                        ScheduledExecutorService.class, SingleDaemonThreadScheduledExectorFactory.getInstance(),
                        ExecutorService.class, ApplicationExecutorFactory.getInstance()));
    }

    protected static final TimeValue DEFAULT_SHUTDOWN_TIMEOUT = TimeValue.create(30L, TimeUnit.SECONDS);
    
    protected final Factory<? extends Publisher> publishers;
    protected final ServiceMonitor serviceMonitor;
    protected final Configuration configuration;
    protected final ListeningExecutorServiceFactory executors;
    protected final TimeValue shutdownTimeout;

    public DefaultRuntimeModule() {
        this(DefaultConfigurationFactory.getInstance().get(DefaultArgumentsFactory.getInstance().get()));
    }

    public DefaultRuntimeModule(
            Configuration configuration) {
        this(configuration, 
                EventBusPublisherFactory.getInstance(), 
                ServiceMonitor.newInstance(), 
                DEFAULT_SHUTDOWN_TIMEOUT);
    }

    public DefaultRuntimeModule(
            Configuration configuration,
            Factory<? extends Publisher> publishers,
            ServiceMonitor serviceMonitor,
            TimeValue shutdownTimeout) {
        this.configuration = configuration;
        this.publishers = publishers;
        this.serviceMonitor = serviceMonitor;
        this.executors = listeningExecutors(serviceMonitor);
        this.shutdownTimeout = shutdownTimeout;
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    @Override
    public Factory<ThreadFactory> threadFactory() {
        return PlatformThreadFactory.getInstance();
    }

    @Override
    public ServiceMonitor serviceMonitor() {
        return serviceMonitor;
    }

    @Override
    public Factory<? extends Publisher> publisherFactory() {
        return publishers;
    }

    @Override
    public ListeningExecutorServiceFactory executors() {
        return executors;
    }
    
    public void shutdown() {
        serviceMonitor().stopAsync();
        try {
            serviceMonitor().awaitTerminated(shutdownTimeout.value(), shutdownTimeout.unit());
        } catch (Exception e) {}
    }
}
