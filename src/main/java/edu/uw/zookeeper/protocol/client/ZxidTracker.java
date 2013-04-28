package edu.uw.zookeeper.protocol.client;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.Reference;

public class ZxidTracker implements Reference<Long> {

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
    
    @Subscribe
    public void handleMessage(Operation.SessionReply message) {
        // Possibly lossy (non-atomic) update of last zxid seen
        // done this way to ensure that we don't accidentally overwrite
        // a higher zxid
        long newZxid = message.zxid();
        long prevZxid = lastZxid.get();
        if (prevZxid < newZxid) {
            lastZxid.compareAndSet(prevZxid, newZxid);
        }
    }
}
