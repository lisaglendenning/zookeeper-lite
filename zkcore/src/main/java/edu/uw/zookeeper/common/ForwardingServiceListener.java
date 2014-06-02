package edu.uw.zookeeper.common;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Service.State;

public class ForwardingServiceListener<T extends Service> extends Service.Listener implements Supplier<T> {

    public static <T extends Service> ForwardingServiceListener<T> forService(T service) {
        return new ForwardingServiceListener<T>(service);
    }
    
    protected final T service;
    
    public ForwardingServiceListener(T service) {
        this.service = service;
    }
    
    @Override
    public T get() {
        return service;
    }
    
    @Override
    public void starting() {
        Services.startAndWait(get());
    }
    
    @Override
    public void terminated(State from) {
        Services.stopAndWait(get());
    }
}