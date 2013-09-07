package edu.uw.zookeeper;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ListeningExecutorServiceFactory;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.SimpleArguments;
import edu.uw.zookeeper.common.TimeValue;


public class DefaultRuntimeModule implements RuntimeModule {

    public static DefaultRuntimeModule defaults() {
        return fromArgs(new String[0]);
    }

    public static DefaultRuntimeModule fromArgs(String[] args) {
        return fromConfiguration(configuration(args));
    }
    
    public static DefaultRuntimeModule fromConfiguration(Configuration configuration) {
        return new DefaultRuntimeModule(
                configuration,
                ServiceMonitor.newInstance(), 
                ListeningExecutorServiceFactory.newInstance(
                        DefaultApplicationExecutorFactory.configured(configuration), 
                        SingleDaemonThreadScheduledExectorFactory.defaults()),
                DEFAULT_SHUTDOWN_TIMEOUT);
    }
    
    public static SimpleArguments arguments(String[] args) {
        return SimpleArguments.create("", "", args);
    }

    public static Configuration configuration(String[] args) {
        return Configuration.defaults(arguments(args));
    }
    
    public static int availableProcessors() {
        return Math.max(1,
                Runtime.getRuntime().availableProcessors());
    }

    public static enum PlatformThreadFactory implements Factory<ThreadFactory> {
        PLATFORM_THREAD_FACTORY;

        public static PlatformThreadFactory getInstance() {
            return PLATFORM_THREAD_FACTORY;
        }
        
        @Override
        public ThreadFactory get() {
            return MoreExecutors.platformThreadFactory();
        }
    }
    
    public static class SingleDaemonThreadScheduledExectorFactory implements Factory<ScheduledExecutorService> {

        public static SingleDaemonThreadScheduledExectorFactory defaults() {
            return fromThreadFactory(PlatformThreadFactory.getInstance().get());
        }
        
        public static SingleDaemonThreadScheduledExectorFactory fromThreadFactory(ThreadFactory threadFactory) {
            return new SingleDaemonThreadScheduledExectorFactory(
                    new ThreadFactoryBuilder()
                    .setThreadFactory(threadFactory)
                    .setDaemon(true)
                    .setNameFormat("scheduled-%d"));
        }
        
        private final ThreadFactoryBuilder threadFactory;

        protected SingleDaemonThreadScheduledExectorFactory(ThreadFactoryBuilder threadFactory) {
            this.threadFactory = threadFactory;
        }
        
        @Override
        public ScheduledExecutorService get() {
            ScheduledExecutorService instance = Executors.newSingleThreadScheduledExecutor(
                    threadFactory.build());
            //MoreExecutors.addDelayedShutdownHook(instance, 100, TimeUnit.MILLISECONDS);
            return instance;
        }
    }
    
    @Configurable(path="Runtime", key="PoolSize", type=ConfigValueType.NUMBER)
    public static class DefaultApplicationExecutorFactory implements Factory<ExecutorService> {

        public static DefaultApplicationExecutorFactory configured(
                Configuration configuration) {
            return fromThreadFactory(configuration, PlatformThreadFactory.getInstance().get());

        }

        public static DefaultApplicationExecutorFactory fromThreadFactory(
                Configuration configuration, ThreadFactory threadFactory) {
            Configurable configurable = DefaultApplicationExecutorFactory.class.getAnnotation(Configurable.class);
            Config config = configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path());
            int corePoolSize = config.hasPath(configurable.key()) ? config.getInt(configurable.key()) : availableProcessors();
            int maxPoolSize = corePoolSize;
            return new DefaultApplicationExecutorFactory(
                    corePoolSize,
                    maxPoolSize,
                    TimeValue.seconds(60),
                    new ThreadFactoryBuilder().setThreadFactory(threadFactory).setNameFormat("main-pool-%d"));
        }

        private final int corePoolSize;
        private final int maxPoolSize;
        private final TimeValue keepAlive;
        private final ThreadFactoryBuilder threadFactory;

        protected DefaultApplicationExecutorFactory(
                int corePoolSize,
                int maxPoolSize,
                TimeValue keepAlive,
                ThreadFactoryBuilder threadFactory) {
            this.corePoolSize = corePoolSize;
            this.maxPoolSize = maxPoolSize;
            this.keepAlive = keepAlive;
            this.threadFactory = threadFactory;
        }

        @Override
        public ExecutorService get() {
            ThreadPoolExecutor executor = new ThreadPoolExecutor(
                    corePoolSize, maxPoolSize,
                    keepAlive.value(), keepAlive.unit(),
                    new LinkedBlockingQueue<Runnable>(),
                    threadFactory.build());
            executor.prestartAllCoreThreads();
            return executor;
        }
    }
    
    protected static final TimeValue DEFAULT_SHUTDOWN_TIMEOUT = TimeValue.create(30L, TimeUnit.SECONDS);
    
    protected final ServiceMonitor serviceMonitor;
    protected final Configuration configuration;
    protected final ListeningExecutorServiceFactory executors;
    protected final TimeValue shutdownTimeout;

    protected DefaultRuntimeModule(
            Configuration configuration,
            ServiceMonitor serviceMonitor,
            ListeningExecutorServiceFactory executors,
            TimeValue shutdownTimeout) {
        this.configuration = configuration;
        this.serviceMonitor = serviceMonitor;
        this.executors = executors;
        this.shutdownTimeout = shutdownTimeout;
        serviceMonitor.add(executors);
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public Factory<ThreadFactory> getThreadFactory() {
        return PlatformThreadFactory.getInstance();
    }

    @Override
    public ServiceMonitor getServiceMonitor() {
        return serviceMonitor;
    }

    @Override
    public ListeningExecutorServiceFactory getExecutors() {
        return executors;
    }

    @Override
    public void shutdown() {
        getServiceMonitor().stopAsync();
        try {
            getServiceMonitor().awaitTerminated(shutdownTimeout.value(), shutdownTimeout.unit());
        } catch (Exception e) {}
    }
}
