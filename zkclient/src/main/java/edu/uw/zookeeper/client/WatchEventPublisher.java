package edu.uw.zookeeper.client;

import net.engio.mbassy.PubSubSupport;
import net.engio.mbassy.listener.Handler;

import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;

// Republishes a notification message as a WatchEvent
public class WatchEventPublisher implements PubSubSupport<WatchEvent> {
    
    public static WatchEventPublisher create(PubSubSupport<? super WatchEvent> publisher, PubSubSupport<?> eventful) {
        WatchEventPublisher instance = new WatchEventPublisher(publisher);
        eventful.subscribe(instance);
        return instance;
    }
    
    protected final PubSubSupport<? super WatchEvent> publisher;
    
    public WatchEventPublisher(PubSubSupport<? super WatchEvent> publisher) {
        this.publisher = publisher;
    }

    @Override
    public void subscribe(Object listener) {
        publisher.subscribe(listener);
    }

    @Override
    public boolean unsubscribe(Object listener) {
        return publisher.unsubscribe(listener);
    }

    @Override
    public void publish(WatchEvent event) {
        publisher.publish(event);
    }
    
    @Handler
    public void handleReply(Operation.ProtocolResponse<?> message) {
        if (OpCodeXid.NOTIFICATION.xid() == message.xid()) {
            WatchEvent event = WatchEvent.fromRecord((IWatcherEvent) message.record());
            publish(event);
        }
    }
}
