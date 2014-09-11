package edu.uw.zookeeper.protocol;

import java.util.concurrent.TimeUnit;

import com.google.common.base.MoreObjects;

/**
 * Not thread-safe
 */
public class TimeOutParameters {

    public static TimeOutParameters milliseconds(long timeOut) {
        return new TimeOutParameters(System.currentTimeMillis(), timeOut);
    }
    
    private long timeOut;
    private long touch;
    
    protected TimeOutParameters(long touch, long timeOut) {
        this.timeOut = timeOut;
        this.touch = touch;
    }
    
    public TimeUnit getUnit() {
        return TimeUnit.MILLISECONDS;
    }

    public long getNow() {
        return System.currentTimeMillis();
    }
    
    public long getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(long timeOut) {
        this.timeOut = timeOut;
    }
    
    public long getTouch() {
        return touch;
    }

    public void setTouch(long touch) {
        this.touch = touch;
    }

    public void setTouch() {
        setTouch(getNow());
    }
    
    public long getRemaining() {
        return touch + timeOut - getNow();
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("timeOut", getTimeOut()).add("remaining", getRemaining()).toString();
    }
}