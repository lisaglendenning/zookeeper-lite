package edu.uw.zookeeper;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.EventBusPublisher;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ListeningExecutorServiceFactory;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.ServiceMonitor;

public abstract class Factories {

    public static enum PublisherFactory implements Factory<Publisher> {
        INSTANCE;

        public static PublisherFactory getInstance() {
            return INSTANCE;
        }

        @Override
        public Publisher get() {
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
            return Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                    .setThreadFactory(threadFactory)
                    .setDaemon(true)
                    .setNameFormat(nameFormat)
                    .build());
        }
    }
    
    public static enum ApplicationExecutorFactory implements DefaultsFactory<ThreadFactory, ExecutorService> {
        INSTANCE;

        public static ApplicationExecutorFactory getInstance() {
            return INSTANCE;
        }
        
        // TODO: configurable
        private final int CORE_SIZE = Math.max(1,
                Runtime.getRuntime().availableProcessors() * 2);
        private final String nameFormat = "application-%d";

        @Override
        public ExecutorService get() {
            return get(PlatformThreadFactory.getInstance().get());
        }
        
        public ExecutorService get(ThreadFactory threadFactory) {
            return Executors.newFixedThreadPool(CORE_SIZE,
                    new ThreadFactoryBuilder()
                        .setThreadFactory(threadFactory)
                        .setNameFormat(nameFormat)
                        .build());
        }
    }
    
    public static ListeningExecutorServiceFactory listeningExecutors(ServiceMonitor serviceMonitor) {
        return ListeningExecutorServiceFactory.newInstance(
                serviceMonitor,
                ImmutableMap.<Class<? extends ExecutorService>, Factory<? extends ExecutorService>>of(
                        ScheduledExecutorService.class, SingleDaemonThreadScheduledExectorFactory.getInstance(),
                        ExecutorService.class, ApplicationExecutorFactory.getInstance()));
    }
}
