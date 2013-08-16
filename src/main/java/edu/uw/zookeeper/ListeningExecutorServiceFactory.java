package edu.uw.zookeeper;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.ExecutorServiceMonitor;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.Singleton;

/**
 * Adds Listening wrapper around executor factory, and maps both a listening
 * and non-listening interface to the same backing Executor.
 */
public class ListeningExecutorServiceFactory extends ExecutorServiceFactory {

    public static ListeningExecutorServiceFactory newInstance(
            ServiceMonitor serviceMonitor,
            Map<Class<? extends ExecutorService>, Factory<? extends ExecutorService>> factories) {
        return new ListeningExecutorServiceFactory(serviceMonitor, factories);
    }
    
    protected ListeningExecutorServiceFactory(
            ServiceMonitor serviceMonitor,
            Map<Class<? extends ExecutorService>, Factory<? extends ExecutorService>> factories) {
        super(serviceMonitor, factories);
    }
    
    public Singleton<ListeningExecutorService> asListeningExecutorServiceFactory() {
        return newView(ListeningExecutorService.class);
    }

    public Singleton<ListeningScheduledExecutorService> asListeningScheduledExecutorServiceFactory() {
        return newView(ListeningScheduledExecutorService.class);
    }
    
    @Override
    protected ExecutorServiceMonitor<?> newInstance(Class<? extends ExecutorService> type) {
        // create executor
        ExecutorService executor = factory.get(type);
        if (executor == null) {
            if (ListeningScheduledExecutorService.class.isAssignableFrom(type)) {
                executor = factory.get(ScheduledExecutorService.class);
            } else if (ListeningExecutorService.class.isAssignableFrom(type)) {
                executor = factory.get(ExecutorService.class);
            } else {
                if (ScheduledExecutorService.class.isAssignableFrom(type)) {
                    executor = factory.get(ListeningScheduledExecutorService.class);
                } else {
                    executor = factory.get(ListeningExecutorService.class);
                }       
            }
        }
        checkArgument(executor != null);
        
        // wrap with listening interface
        if (! (executor instanceof ListeningExecutorService)) {
            if (ScheduledExecutorService.class.isAssignableFrom(type)) {
                executor = MoreExecutors.listeningDecorator((ScheduledExecutorService)executor);
            } else {
                executor = MoreExecutors.listeningDecorator((ExecutorService)executor);
            }
        }
        
        // wrap with service
        ExecutorServiceMonitor<?> instance = ExecutorServiceMonitor.newInstance(executor);
        
        // add extra lookup
        Class<? extends ExecutorService> extraType;
        if (ScheduledExecutorService.class.isAssignableFrom(type)) {
            if (ListeningScheduledExecutorService.class.isAssignableFrom(type)) {
                extraType = ScheduledExecutorService.class;
            } else {
                extraType = ListeningScheduledExecutorService.class;
            }
        } else {
            if (ListeningExecutorService.class.isAssignableFrom(type)) {
                extraType = ExecutorService.class;
            } else {
                extraType = ListeningExecutorService.class;
            }
        }
        assert(! type.equals(extraType));
        checkArgument(! instances.containsKey(extraType));
        instances.put(extraType, instance);
        
        return instance;
    }
}