package org.apache.zookeeper.util;


public class EventfulBridge extends ForwardingEventful {

    protected final Eventful bridged;
    
    public EventfulBridge(Eventful internal, Eventful bridged) {
        super(internal);
        this.bridged = bridged;
    }
    
    public Eventful bridged() {
        return bridged();
    }
    
    @Override
    public void post(Object event) {
        super.post(event);
        bridged.post(event);
    }
}