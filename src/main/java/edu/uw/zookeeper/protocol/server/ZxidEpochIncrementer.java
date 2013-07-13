package edu.uw.zookeeper.protocol.server;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.atomic.AtomicLong;



/**
 * zxid composed of a 32 bit epoch (MSB) and a 32 bit counter (LSB).
 * 
 * Threadsafe
 */
public class ZxidEpochIncrementer extends ZxidIncrementer {

    // TODO: it seems like epoch and counter should be integers,
    // but I'm not sure how this all interacts with signedness
    // so for now we just copy ZxidUtil
    
    public static long epochOf(long zxid) {
        return zxid >> 32L;
    }

    public static long counterOf(long zxid) {
        return zxid & 0xffffffffL;
    }

    public static long zxidOf(long epoch) {
        return zxidOf(epoch, 0);
    }
    
    public static long zxidOf(long epoch, long counter) {
        return (epoch << 32L) | (counter & 0xffffffffL);
    }
    
    public static ZxidEpochIncrementer fromZero() {
        return fromEpoch(0);
    }

    public static ZxidEpochIncrementer fromEpoch(long epoch) {
        return of(new AtomicLong(zxidOf(epoch)));
    }

    public static ZxidEpochIncrementer of(AtomicLong lastZxid) {
        checkArgument(lastZxid.get() >= 0);
        return new ZxidEpochIncrementer(lastZxid);
    }

    protected ZxidEpochIncrementer(AtomicLong lastZxid) {
        super(lastZxid);
    }
    
    public long epoch() {
        return epochOf(get());
    }
    
    public long counter() {
        return counterOf(get());
    }
    
    public boolean setEpoch(long nextEpoch) {
        long current = get();
        long nextZxid = zxidOf(nextEpoch);
        checkArgument(nextZxid > current);
        return lastZxid.compareAndSet(current, nextZxid);
    }
    
    // TODO: check for counter overflow into epoch bits
//    @Override
//    public Long next() {
//        return lastZxid.incrementAndGet();
//    }

}
