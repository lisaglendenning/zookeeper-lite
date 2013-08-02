package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.atomic.AtomicReference;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.Reference;

public class StampedReference<T> extends AbstractPair<Long, T> implements Reference<T> {

    public static <T> StampedReference<T> of(T value) {
        return new StampedReference<T>(Long.valueOf(0), value);
    }

    public static <T> StampedReference<T> of(Long stamp, T value) {
        return new StampedReference<T>(stamp, value);
    }

    public static class Updater<T> implements Reference<StampedReference<T>> {
        public static <T> Updater<T> newInstance(StampedReference<T> value) {
            return new Updater<T>(value);
        }

        protected final AtomicReference<StampedReference<T>> reference;

        protected Updater(StampedReference<T> value) {
            this.reference = new AtomicReference<StampedReference<T>>(checkNotNull(value));
        }
        
        public StampedReference<T> setIfGreater(StampedReference<T> value) {
            StampedReference<T> current = reference.get();
            if (current.stamp() < checkNotNull(value).stamp()) {
                if (! reference.compareAndSet(current, value)) {
                    return setIfGreater(value);
                }
            }
            return current;
        }
        
        @Override
        public StampedReference<T> get() {
            return reference.get();
        }
        
        @Override
        public String toString() {
            return reference.toString();
        }
    }
    
    protected StampedReference(Long stamp, T value) {
        super(stamp, value);
    }
    
    public Long stamp() {
        return first;
    }
    
    @Override
    public T get() {
        return second;
    }
}
