package edu.uw.zookeeper.util;

import com.google.common.eventbus.EventBus;

/**
 * Retrofits Eventful interface onto EventBus.
 */
public class EventfulEventBus extends EventBus implements Eventful {

    public EventfulEventBus() {
        super();
    }
}
