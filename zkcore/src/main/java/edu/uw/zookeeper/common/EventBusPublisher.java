package edu.uw.zookeeper.common;

import com.google.common.eventbus.EventBus;

/**
 * Retrofits Publisher interface onto EventBus.
 */
public class EventBusPublisher extends EventBus implements Publisher {

    public static EventBusPublisher newInstance() {
        return new EventBusPublisher();
    }

    public static EventBusPublisher newInstance(String identifier) {
        return new EventBusPublisher(identifier);
    }

    public static Factory<EventBusPublisher> factory() {
        return new Factory<EventBusPublisher>() {
            @Override
            public EventBusPublisher get() {
                return new EventBusPublisher();
            }     
        };
    }
    
    public EventBusPublisher() {
        super();
    }

    public EventBusPublisher(String identifier) {
        super(identifier);
    }
}
