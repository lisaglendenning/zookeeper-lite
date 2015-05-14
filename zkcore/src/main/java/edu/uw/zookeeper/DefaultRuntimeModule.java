package edu.uw.zookeeper;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ListeningExecutorServiceFactory;
import edu.uw.zookeeper.common.Pair;
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
        return create(
                configuration,
                ServiceMonitor.defaults(), 
                ListeningExecutorServiceFactory.newInstance(
                        DefaultApplicationExecutorFactory.fromConfiguration(configuration), 
                        SingleThreadScheduledExectorFactory.defaults()),
                ShutdownTimeoutConfiguration.get(configuration));
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
    
    public static class ThreadPoolExecutorPurger extends Pair<ScheduledExecutorService, ThreadPoolExecutor> implements Runnable {

        public static <T extends ThreadPoolExecutor> T purge(ScheduledExecutorService scheduler, T executor) {
            return purge(scheduler, executor, DEFAULT_PURGE_INTERVAL);
        }
        
        public static <T extends ThreadPoolExecutor> T purge(ScheduledExecutorService scheduler, T executor, TimeValue interval) {
            ThreadPoolExecutorPurger purger = new ThreadPoolExecutorPurger(scheduler, executor);
            scheduler.scheduleAtFixedRate(purger, interval.value(), interval.value(), interval.unit());
            return executor;
        }
        
        private static final TimeValue DEFAULT_PURGE_INTERVAL = TimeValue.seconds(30);
        
        public ThreadPoolExecutorPurger(ScheduledExecutorService scheduler, ThreadPoolExecutor executor) {
            super(scheduler, executor);
        }
        
        @Override
        public void run() {
            second.purge();
        }
    }

    public static class SingleThreadScheduledExectorFactory implements Factory<ScheduledExecutorService> {

        public static SingleThreadScheduledExectorFactory defaults() {
            return defaults(PlatformThreadFactory.getInstance().get());
        }
        
        public static SingleThreadScheduledExectorFactory defaults(ThreadFactory threadFactory) {
            return fromThreadFactoryBuilder(
                    new ThreadFactoryBuilder()
                    .setThreadFactory(threadFactory)
                    .setDaemon(true)
                    .setNameFormat("scheduled-%d"));
        }
        
        public static SingleThreadScheduledExectorFactory fromThreadFactoryBuilder(ThreadFactoryBuilder threadFactory) {
            return new SingleThreadScheduledExectorFactory(threadFactory);
        }
        
        private final ThreadFactoryBuilder threadFactory;

        protected SingleThreadScheduledExectorFactory(ThreadFactoryBuilder threadFactory) {
            this.threadFactory = threadFactory;
        }
        
        @Override
        public ScheduledThreadPoolExecutor get() {
            ScheduledThreadPoolExecutor instance = new ScheduledThreadPoolExecutor(1, threadFactory.build());
            // not available until Java 7 :-(
            // instance.setRemoveOnCancelPolicy(true);
            instance.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            instance.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            //MoreExecutors.addDelayedShutdownHook(instance, 100, TimeUnit.MILLISECONDS);
            ThreadPoolExecutorPurger.purge(instance, instance);
            return instance;
        }
    }
    
    @Configurable(path="runtime", key="poolSize", value="0", type=ConfigValueType.NUMBER)
    public static class DefaultApplicationExecutorFactory implements Factory<ExecutorService> {

        public static DefaultApplicationExecutorFactory fromConfiguration(
                Configuration configuration) {
            return fromThreadFactory(configuration, PlatformThreadFactory.getInstance().get());
        }

        public static DefaultApplicationExecutorFactory fromThreadFactory(
                Configuration configuration, ThreadFactory threadFactory) {
            int corePoolSize = getCorePoolSize(configuration, DefaultApplicationExecutorFactory.class);
            int maxPoolSize = corePoolSize;
            return new DefaultApplicationExecutorFactory(
                    corePoolSize,
                    maxPoolSize,
                    TimeValue.seconds(60),
                    new ThreadFactoryBuilder().setThreadFactory(threadFactory).setNameFormat("main-pool-%d"));
        }
        
        protected static int getCorePoolSize(
                Configuration configuration,
                Class<?> cls) {
            Configurable configurable = cls.getAnnotation(Configurable.class);
            Config config = configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path());
            int corePoolSize = config.getInt(configurable.key());
            if (corePoolSize == 0) {
                corePoolSize = availableProcessors();
            }
            return corePoolSize;
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
            return new ThreadPoolExecutor(
                    corePoolSize, maxPoolSize,
                    keepAlive.value(), keepAlive.unit(),
                    Queues.<Runnable>newLinkedBlockingQueue(),
                    threadFactory.build());
        }
    }

    @Configurable(path="runtime", key="shutdown", value="30 seconds", type=ConfigValueType.STRING)
    public static abstract class ShutdownTimeoutConfiguration {
        
        public static TimeValue get(Configuration configuration) {
            Configurable configurable = ShutdownTimeoutConfiguration.class.getAnnotation(Configurable.class);
            Config config = configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path());
            return TimeValue.fromString(config.getString(configurable.key()));
        }
    }
    
    public static DefaultRuntimeModule create(
            Configuration configuration,
            ServiceMonitor serviceMonitor,
            ListeningExecutorServiceFactory executors,
            TimeValue shutdownTimeout) {
        return new DefaultRuntimeModule(configuration, serviceMonitor, executors, shutdownTimeout);
    }
    
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
