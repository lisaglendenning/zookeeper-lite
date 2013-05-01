package edu.uw.zookeeper.protocol.server;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.atomic.AtomicLong;

import edu.uw.zookeeper.util.Generator;

/**
 * Generates increasing zxid starting with 1.
 * 
 * Threadsafe
 */
public class ZxidIncrementer implements Generator<Long> {

    public static ZxidIncrementer newInstance() {
        return new ZxidIncrementer(new AtomicLong(0));
    }
    
    public static ZxidIncrementer newInstance(AtomicLong lastZxid) {
        return new ZxidIncrementer(lastZxid);
    }

    private final AtomicLong lastZxid;
    
    protected ZxidIncrementer(AtomicLong lastZxid) {
        super();
        checkArgument(lastZxid.get() >= 0);
        this.lastZxid = lastZxid;
    }

    @Override
    public Long get() {
        return lastZxid.get();
    }

    @Override
    public Long next() {
        return lastZxid.incrementAndGet();
    }
}
