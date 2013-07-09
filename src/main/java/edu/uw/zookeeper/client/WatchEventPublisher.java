package edu.uw.zookeeper.client;

import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.ForwardingEventful;
import edu.uw.zookeeper.util.Publisher;

// Republishes a notification message as a WatchEvent
public class WatchEventPublisher extends ForwardingEventful {
    
    public static WatchEventPublisher newInstance(Publisher publisher, Eventful connection) {
        return new WatchEventPublisher(publisher, connection);
    }
    
    protected final Eventful connection;
    
    public WatchEventPublisher(Publisher publisher, Eventful connection) {
        super(publisher);
        this.connection = connection;
        
        connection.register(this);
    }

    @Subscribe
    public void handleReply(Operation.SessionResponse message) {
        if (OpCodeXid.NOTIFICATION.xid() == message.xid()) {
            WatchEvent event = WatchEvent.of((IWatcherEvent) message.response());
            post(event);
        }
    }        
    
    @SuppressWarnings("unchecked")
    @Subscribe
    public void handleStateEvent(Automaton.Transition<?> event) {
        if (event.type().isAssignableFrom(Connection.State.class)) {
            handleConnectionStateEvent((Automaton.Transition<Connection.State>)event);
        }
    }
    
    public void handleConnectionStateEvent(Automaton.Transition<Connection.State> event) {
        switch (event.to()) {
        case CONNECTION_CLOSED:
            try {
                connection.unregister(this);
            } catch (IllegalArgumentException e) {}
            break;
        default:
            break;
        }
    }
}
