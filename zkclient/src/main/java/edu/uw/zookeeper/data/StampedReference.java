package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Objects;

public final class StampedReference<T> {

    public static <T> StampedReference<T> of(T value) {
        return new StampedReference<T>(0L, value);
    }

    public static <T> StampedReference<T> of(long stamp, T value) {
        return new StampedReference<T>(stamp, value);
    }

    public static final class Updater<T> {
        public static <T> Updater<T> newInstance(StampedReference<T> value) {
            return new Updater<T>(value);
        }

        private StampedReference<T> value;

        protected Updater(StampedReference<T> value) {
            this.value = checkNotNull(value);
        }
        
        public synchronized StampedReference<T> update(StampedReference<T> value) {
            if (this.value.stamp() < checkNotNull(value).stamp()) {
                StampedReference<T> prev = this.value;
                this.value = value;
                return prev;
            } else {
                return this.value;
            }
        }
        
        public synchronized StampedReference<T> get() {
            return value;
        }
        
        @Override
        public synchronized String toString() {
            return value.toString();
        }
    }
    
    private final long stamp;
    private final T value;
    
    protected StampedReference(long stamp, T value) {
        this.stamp = stamp;
        this.value = value;
    }
    
    public long stamp() {
        return stamp;
    }
    
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
        if (! (obj instanceof StampedReference<?>)) {
            return false;
        }
        StampedReference<?> other = (StampedReference<?>) obj;
        return (stamp == other.stamp) && Objects.equal(value, other.value);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(stamp, value);
    }
}
