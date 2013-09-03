package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.*;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class ListeningExecutorServiceFactory extends AbstractIdleService {

    public static ListeningExecutorServiceFactory newInstance(
            Factory<? extends ExecutorService> executorFactory,
            Factory<? extends ScheduledExecutorService> scheduledFactory) {
        return new ListeningExecutorServiceFactory(
                ImmutableMap.<Class<? extends ExecutorService>, Factory<? extends ExecutorService>>of(
                        ExecutorService.class, executorFactory,
                        ScheduledExecutorService.class, scheduledFactory));
    }

    private final Map<Class<? extends ExecutorService>, ExecutorServiceService<? extends ListeningExecutorService>> instances;
    private final Map<Class<? extends ExecutorService>, Factory<? extends ExecutorService>> factories;
    
    protected ListeningExecutorServiceFactory(
            Map<Class<? extends ExecutorService>, Factory<? extends ExecutorService>> factories) {
        this.factories = ImmutableMap.copyOf(factories);
        this.instances = Maps.newHashMap();
    }
    
    @SuppressWarnings("unchecked")
    public synchronized <T extends ExecutorService> T get(Class<T> type) {
        if (instances.containsKey(checkNotNull(type))) {
            return (T) instances.get(type).get();
        }
        Factory<? extends ExecutorService> factory = factories.get(type);
        checkArgument(factory != null);
        ListeningExecutorService instance = MoreExecutors.listeningDecorator(factory.get());
        ImmutableList<Class<? extends ExecutorService>> types;
        if (instance instanceof ScheduledExecutorService) {
            types = ImmutableList.<Class<? extends ExecutorService>>of(
                    ScheduledExecutorService.class,
                    ListeningScheduledExecutorService.class);
        } else {
            types = ImmutableList.<Class<? extends ExecutorService>>of(
                    ExecutorService.class,
                    ListeningExecutorService.class);
        }
        ExecutorServiceService<ListeningExecutorService> service = ExecutorServiceService.newInstance(instance);
        for (Class<? extends ExecutorService> t: types) {
            instances.put(t, service);
        }
        return (T) service.get();
    }

    @Override
    protected synchronized void startUp() throws Exception {
    }

    @Override
    protected synchronized void shutDown() throws Exception {
        for (ExecutorServiceService<?> e: instances.values()) {
            if (e.isRunning()) {
                e.stopAsync().awaitTerminated();;
            }
        }
    }
}