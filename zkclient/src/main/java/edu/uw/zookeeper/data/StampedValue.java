package edu.uw.zookeeper.data;


import com.google.common.base.Objects;
import com.google.common.base.Supplier;

public final class StampedValue<T> implements Supplier<T >{

    public static <T> StampedValue<T> of(long stamp, T value) {
        return new StampedValue<T>(stamp, value);
    }
    
    private final long stamp;
    private final T value;
    
    private StampedValue(long stamp, T value) {
        this.stamp = stamp;
        this.value = value;
    }
    
    public long stamp() {
        return stamp;
    }

    @Override
    public T get() {
        return value;
    }
    
    @Override
    public String toString() {
        return String.format("(%d, %s)", stamp, value);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof StampedValue<?>)) {
            return false;
        }
        StampedValue<?> other = (StampedValue<?>) obj;
        return (stamp == other.stamp) && Objects.equal(value, other.value);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(stamp, value);
    }
}
