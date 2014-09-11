package edu.uw.zookeeper.protocol.server;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.MoreObjects;


/**
 * Generates increasing zxid starting with 1.
 * 
 * Threadsafe
 */
public class ZxidIncrementer implements ZxidGenerator {

    public static ZxidIncrementer fromZero() {
        return of(new AtomicLong(0));
    }
    
    public static ZxidIncrementer of(AtomicLong lastZxid) {
        checkArgument(lastZxid.get() >= 0);
        return new ZxidIncrementer(lastZxid);
    }

    protected final AtomicLong lastZxid;
    
    protected ZxidIncrementer(AtomicLong lastZxid) {
        super();
        this.lastZxid = lastZxid;
    }

    @Override
    public long get() {
        return lastZxid.get();
    }

    @Override
    public long next() {
        return lastZxid.incrementAndGet();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .addValue(String.format("0x%s", Long.toHexString(get())))
                .toString();
    }
}
