package edu.uw.zookeeper.client;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Objects;

import edu.uw.zookeeper.util.Generator;

/**
 * Generates increasing xid starting with 1.
 * 
 * Threadsafe
 */
public class XidIncrementer implements Generator<Integer> {

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
    public Integer get() {
        return lastXid.get();
    }

    @Override
    public Integer next() {
        return lastXid.incrementAndGet();
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .addValue(String.format("0x%s", Integer.toHexString(get())))
                .toString();
    }
}
