package edu.uw.zookeeper;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.collect.Maps;

import edu.uw.zookeeper.common.ExecutorServiceMonitor;
import edu.uw.zookeeper.common.Factories;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.Singleton;

public class ExecutorServiceFactory implements ParameterizedFactory<Class<? extends ExecutorService>, ExecutorService> {

    protected class TypeView<T extends ExecutorService> implements Singleton<T> {
        protected final Class<T> type;
        
        public TypeView(Class<T> type) {
            this.type = type;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public T get() {
            return (T) ExecutorServiceFactory.this.get(type);
        }
    }
    
    public static ExecutorServiceFactory newInstance(
            ServiceMonitor serviceMonitor,
            Map<Class<? extends ExecutorService>, Factory<? extends ExecutorService>> factories) {
        return new ExecutorServiceFactory(serviceMonitor, factories);
    }
    
    protected final ServiceMonitor serviceMonitor;
    protected final Factories.ByTypeFactory<ExecutorService> factory;
    protected final Map<Class<? extends ExecutorService>, ExecutorServiceMonitor<?>> instances;
    
    protected ExecutorServiceFactory(
            ServiceMonitor serviceMonitor,
            Map<Class<? extends ExecutorService>, Factory<? extends ExecutorService>> factories) {
        checkArgument(! factories.isEmpty());
        this.serviceMonitor = serviceMonitor;
        this.factory = Factories.ByTypeFactory.newInstance(factories);
        this.instances = Maps.newHashMap();
    }
    
    public Singleton<ExecutorService> asExecutorServiceFactory() {
        return newView(ExecutorService.class);
    }

    public Singleton<ScheduledExecutorService> asScheduledExecutorServiceFactory() {
        return newView(ScheduledExecutorService.class);
    }
    
    protected <T extends ExecutorService> TypeView<T> newView(Class<T> type) {
        return new TypeView<T>(type);
    }
    
    @Override
    public synchronized ExecutorService get(Class<? extends ExecutorService> type) {
        ExecutorServiceMonitor<?> instance = instances.get(type);
        if (instance == null) {
            instance = newInstance(type);
            instances.put(type, instance);
            serviceMonitor.add(instance);
        }
        return instance.get();
    }
    
    protected ExecutorServiceMonitor<?> newInstance(Class<? extends ExecutorService> type) {
        ExecutorService executor = factory.get(type);
        checkArgument(executor != null);
        ExecutorServiceMonitor<?> instance = ExecutorServiceMonitor.newInstance(executor);
        return instance;
    }
}