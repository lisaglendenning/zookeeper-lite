package edu.uw.zookeeper.protocol.client;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.server.ZxidReference;

public class ZxidTracker implements ZxidReference  {
    
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
        public void handleSessionReply(Operation.ProtocolResponse<?> message) {
            first().update(message.getZxid());
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

    @Override
    public long get() {
        return lastZxid.get();
    }
    
    public boolean update(long zxid) {
        long prevZxid = lastZxid.get();
        if (prevZxid < zxid) {
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
