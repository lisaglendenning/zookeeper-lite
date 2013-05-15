package edu.uw.zookeeper.protocol.client;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Reference;

public class ZxidTracker implements Reference<Long>  {

    public static class Decorator implements Factory<ClientCodecConnection> {
        public static Decorator newInstance(
                Factory<? extends ClientCodecConnection> delegate) {
            return newInstance(delegate, ZxidTracker.create());
        }
        
        public static Decorator newInstance(
                Factory<? extends ClientCodecConnection> delegate,
                ZxidTracker tracker) {
            return new Decorator(delegate, tracker);
        }
        
        private final ZxidTracker tracker;
        private final Factory<? extends ClientCodecConnection> delegate;
        
        private Decorator(
                Factory<? extends ClientCodecConnection> delegate,
                ZxidTracker tracker) {
            this.delegate = delegate;
            this.tracker = tracker;
        }
        
        public ZxidTracker asTracker() {
            return tracker;
        }
        
        @Override
        public ClientCodecConnection get() {
            ClientCodecConnection client = delegate.get();
            // TODO: unregister when connection closes...
            client.register(this);
            return client;
        }
        
        @Subscribe
        public void handleMessage(Operation.SessionReply message) {
            tracker.update(message.zxid());
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
