package edu.uw.zookeeper.util;

import com.google.common.eventbus.EventBus;

/**
 * Retrofits Publisher interface onto EventBus.
 */
public class EventBusPublisher extends EventBus implements Publisher {

    public EventBusPublisher() {
        super();
    }
}
