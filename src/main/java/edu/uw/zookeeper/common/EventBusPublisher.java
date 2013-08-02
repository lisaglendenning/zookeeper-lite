package edu.uw.zookeeper.common;

import com.google.common.eventbus.EventBus;

/**
 * Retrofits Publisher interface onto EventBus.
 */
public class EventBusPublisher extends EventBus implements Publisher {

    public static EventBusPublisher newInstance() {
        return new EventBusPublisher();
    }
    
    public EventBusPublisher() {
        super();
    }
}
