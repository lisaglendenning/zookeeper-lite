package edu.uw.zookeeper.protocol.client;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Reference;

public class ZxidTracker implements Reference<Long>  {
    
    public static class ZxidListener extends Pair<ZxidTracker, Eventful> {

        public static ZxidListener create(
                ZxidTracker tracker,
                Eventful eventful) {
            return new ZxidListener(tracker, eventful);
        }
        
        public ZxidListener(ZxidTracker tracker, Eventful eventful) {
            super(tracker, eventful);
            eventful.register(this);
        }

        @Subscribe
        public void handleTransition(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                try {
                    second().unregister(this);
                } catch (IllegalArgumentException e) {}
            }
        }
        
        @Subscribe
        public void handleSessionReply(Operation.SessionResponse message) {
            first().update(message.zxid());
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
        Long prevZxid = lastZxid.get();
        if (prevZxid.compareTo(zxid) < 0) {
            if (lastZxid.compareAndSet(prevZxid, zxid)) {
                return true;
            } else {
                // try again
                return update(zxid);
            }
        } else {
            return false;
        }
    }
}
