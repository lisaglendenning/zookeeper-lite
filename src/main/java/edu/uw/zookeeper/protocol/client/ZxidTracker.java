package edu.uw.zookeeper.protocol.client;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.AbstractPair;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Reference;

public class ZxidTracker implements Reference<Long>  {
    
    public static class ZxidListener<I, C extends Connection<I>> extends AbstractPair<ZxidTracker, C> {

        public static <I, C extends Connection<I>> ZxidListener<I,C> newInstance(
                ZxidTracker tracker,
                C connection) {
            return new ZxidListener<I,C>(tracker, connection);
        }
        
        public ZxidListener(ZxidTracker tracker, C connection) {
            super(tracker, connection);
            connection.register(this);
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
                    second.unregister(this);
                } catch (IllegalArgumentException e) {}
                break;
            default:
                break;
            }
        }

        @Subscribe
        public void handleSessionReply(Operation.SessionReply message) {
            first.update(message.zxid());
        }
    }

    public static class Decorator<I, C extends Connection<I>> 
            extends Pair<ZxidTracker, Factory<C>> 
            implements Factory<C> {
        public static <I, C extends Connection<I>> Decorator<I,C> newInstance(
                Factory<C> delegate) {
            return newInstance(ZxidTracker.create(), delegate);
        }
        
        public static <I, C extends Connection<I>> Decorator<I,C> newInstance(
                ZxidTracker tracker,
                Factory<C> delegate) {
            return new Decorator<I,C>(tracker, delegate);
        }
        
        public Decorator(
                ZxidTracker tracker,
                Factory<C> delegate) {
            super(tracker, delegate);
        }
        
        @Override
        public C get() {
            C connection = second().get();
            ZxidListener.newInstance(first(), connection);
            return connection;
        }
        
    }
    
    public static ZxidTracker create() {
        return new ZxidTracker(new AtomicLong(0));
    }
    
    public static ZxidTracker create(AtomicLong lastZxid) {
        return new ZxidTracker(lastZxid);
    }
    
    protected final AtomicLong lastZxid;
    
    protected ZxidTracker(AtomicLong lastZxid) {
        super();
        this.lastZxid = lastZxid;
    }

    public Long get() {
        return lastZxid.get();
    }
    
    public boolean update(Long zxid) {
        // Possibly lossy attempt to update of last zxid seen
        // done this way to ensure that we don't accidentally overwrite
        // a higher zxid
        Long prevZxid = lastZxid.get();
        if (prevZxid.compareTo(zxid) < 0) {
            return lastZxid.compareAndSet(prevZxid, zxid);
        } else {
            return false;
        }
    }
}
