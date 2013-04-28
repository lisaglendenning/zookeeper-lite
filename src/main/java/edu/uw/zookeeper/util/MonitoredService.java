package edu.uw.zookeeper.util;

import com.google.common.util.concurrent.Service;


public class MonitoredService<T extends Service> implements Factory<T> {

    public static <T extends Service> MonitoredService<T> newInstance(
            Factory<T> serviceFactory,
            ServiceMonitor serviceMonitor) {
        return new MonitoredService<T>(serviceFactory, serviceMonitor);
    }
    
    protected final Factory<T> serviceFactory;
    protected final ServiceMonitor serviceMonitor;
    
    protected MonitoredService(
            Factory<T> serviceFactory,
            ServiceMonitor serviceMonitor) {
        this.serviceFactory = serviceFactory;
        this.serviceMonitor = serviceMonitor;
    }

    @Override
    public T get() {
        T service = serviceFactory.get();
        serviceMonitor.add(service);
        return service;
    }
}