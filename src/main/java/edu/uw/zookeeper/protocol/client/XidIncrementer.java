package edu.uw.zookeeper.protocol.client;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.atomic.AtomicInteger;

import edu.uw.zookeeper.util.Generator;

/**
 * Generates increasing xid starting with 1.
 * 
 * Threadsafe
 */
public class XidIncrementer implements Generator<Integer> {

    public static XidIncrementer newInstance() {
        return new XidIncrementer(new AtomicInteger(0));
    }
    
    public static XidIncrementer newInstance(AtomicInteger lastXid) {
        return new XidIncrementer(lastXid);
    }

    private final AtomicInteger lastXid;
    
    protected XidIncrementer(AtomicInteger lastXid) {
        super();
        // some xid < 0 are reserved
        checkArgument(lastXid.get() >= 0);
        this.lastXid = lastXid;
    }

    @Override
    public Integer get() {
        return lastXid.get();
    }

    @Override
    public Integer next() {
        return lastXid.incrementAndGet();
    }
}
