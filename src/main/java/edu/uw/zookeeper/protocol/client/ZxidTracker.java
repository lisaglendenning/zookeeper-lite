package edu.uw.zookeeper.protocol.client;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Reference;

public class ZxidTracker implements Reference<Long>  {
    
    public static class ZxidListener extends Pair<ZxidTracker, Eventful> {

        public static ZxidListener newInstance(
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
        public void handleSessionReply(Operation.SessionReply message) {
            first().update(message.zxid());
        }
    }

    public static class Decorator<C extends Eventful> 
            extends Pair<ZxidTracker, Factory<C>> 
            implements Factory<C> {
        public static <C extends Eventful> Decorator<C> newInstance(
                Factory<C> delegate) {
            return newInstance(ZxidTracker.create(), delegate);
        }
        
        public static <C extends Eventful> Decorator<C> newInstance(
                ZxidTracker tracker,
                Factory<C> delegate) {
            return new Decorator<C>(tracker, delegate);
        }
        
        public Decorator(
                ZxidTracker tracker,
                Factory<C> delegate) {
            super(tracker, delegate);
        }
        
        @Override
        public C get() {
            C instance = second().get();
            ZxidListener.newInstance(first(), instance);
            return instance;
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
