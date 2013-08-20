package edu.uw.zookeeper.protocol;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import edu.uw.zookeeper.common.TimeValue;

public class TimeOutParameters {

    public static TimeOutParameters create(TimeValue timeOut) {
        timeOut = timeOut.convert(TimeUnit.MILLISECONDS);
        return new TimeOutParameters(System.currentTimeMillis(), timeOut.value());
    }
    
    private final AtomicLong timeOut;
    private final AtomicLong nextTimeOut;
    
    protected TimeOutParameters(long now, long timeOut) {
        this.timeOut = new AtomicLong(timeOut);
        this.nextTimeOut = new AtomicLong(now + timeOut);
    }
    
    public TimeUnit getUnit() {
        return TimeUnit.MILLISECONDS;
    }
    
    public long getTimeOut() {
        return timeOut.get();
    }

    public long setTimeOut(long timeOut) {
        return this.timeOut.getAndSet(timeOut);
    }
    
    public long getNextTimeOut() {
        return nextTimeOut.get();
    }

    public void touch() {
        long prev = nextTimeOut.get();
        long next = now() + getTimeOut();
        if (prev < next) {
            nextTimeOut.compareAndSet(prev, next);
        }
    }
    
    public long now() {
        return System.currentTimeMillis();
    }
    
    public long remaining() {
        return getNextTimeOut() - now();
    }
}