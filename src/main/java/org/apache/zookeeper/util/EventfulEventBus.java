package org.apache.zookeeper.util;

import com.google.common.eventbus.EventBus;
import com.google.inject.AbstractModule;

public class EventfulEventBus extends EventBus implements Eventful {
    
    public static class EventfulModule extends AbstractModule {

        public static EventfulModule get() {
            return new EventfulModule();
        }
        
        protected EventfulModule() {}
        
        @Override
        protected void configure() {
            bind(Eventful.class).to(EventfulEventBus.class);
        }
    }
    
    public EventfulEventBus() {
        super();
    }
/*
    protected final EventBus delegate;
    
    public EventfulEventBus() {
        this(new EventBus());
    }
    
    public EventfulEventBus(EventBus delegate) {
        super();
        this.delegate = delegate;
    }
    
    protected EventBus delegate() {
        return this.delegate;
    }

    @Override
    public void post(Object event) {
        delegate().post(event);
    }

    @Override
    public void register(Object object) {
        delegate().register(object);
    }

    @Override
    public void unregister(Object object) {
        delegate().unregister(object);
    }
*/
}
