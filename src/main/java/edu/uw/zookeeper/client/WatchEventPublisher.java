package edu.uw.zookeeper.client;

import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.net.UnregisterOnClose;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.ForwardingEventful;
import edu.uw.zookeeper.util.Publisher;

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
            @SuppressWarnings("unchecked")
            WatchEvent event = WatchEvent.of((Operation.ProtocolResponse<IWatcherEvent>) message);
            post(event);
        }
    }
}
