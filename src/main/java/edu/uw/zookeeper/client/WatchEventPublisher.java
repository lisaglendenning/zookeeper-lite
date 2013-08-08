package edu.uw.zookeeper.client;

import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.ForwardingEventful;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.net.UnregisterOnClose;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;

// Republishes a notification message as a WatchEvent
public class WatchEventPublisher extends ForwardingEventful {
    
    public static WatchEventPublisher create(Publisher publisher, Eventful eventful) {
        WatchEventPublisher instance = new WatchEventPublisher(publisher);
        UnregisterOnClose.create(instance, eventful);
        return instance;
    }
    
    public WatchEventPublisher(Publisher publisher) {
        super(publisher);
    }

    @Subscribe
    public void handleReply(Operation.ProtocolResponse<?> message) {
        if (OpCodeXid.NOTIFICATION.getXid() == message.getXid()) {
            WatchEvent event = WatchEvent.fromRecord((IWatcherEvent) message.getRecord());
            post(event);
        }
    }
}
