package edu.uw.zookeeper.util;

/*
 * Forwards events to a bridged Eventful.
 */
public class EventfulBridge extends ForwardingEventful {

    private final Eventful bridged;

    public EventfulBridge(Eventful primary, Eventful bridged) {
        super(primary);
        this.bridged = bridged;
    }

    public Eventful bridged() {
        return bridged();
    }

    @Override
    public void post(Object event) {
        // The order is important here
        super.post(event);
        bridged.post(event);
    }
}