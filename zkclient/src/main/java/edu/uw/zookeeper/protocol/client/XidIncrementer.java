package edu.uw.zookeeper.protocol.client;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.MoreObjects;

/**
 * Generates increasing xid starting with 1.
 * 
 * Threadsafe
 */
public class XidIncrementer implements XidGenerator {

    public static XidIncrementer fromZero() {
        return of(new AtomicInteger(0));
    }
    
    public static XidIncrementer of(AtomicInteger lastXid) {
        // some xid < 0 are reserved
        checkArgument(lastXid.get() >= 0);
        return new XidIncrementer(lastXid);
    }

    protected final AtomicInteger lastXid;
    
    protected XidIncrementer(AtomicInteger lastXid) {
        super();
        this.lastXid = lastXid;
    }

    @Override
    public int get() {
        return lastXid.get();
    }

    @Override
    public int next() {
        return lastXid.incrementAndGet();
    }
    
    public int setIfGreater(int value) {
        Integer current = lastXid.get();
        if (current.compareTo(value) < 0) {
            if (lastXid.compareAndSet(current, value)) {
                return value;
            } else {
                return setIfGreater(value);
            }
        } else {
            return current;
        }
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .addValue(String.format("0x%s", Integer.toHexString(get())))
                .toString();
    }
}
