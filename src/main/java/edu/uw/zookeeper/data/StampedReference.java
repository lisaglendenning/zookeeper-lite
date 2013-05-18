package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.atomic.AtomicReference;

import edu.uw.zookeeper.util.AbstractPair;
import edu.uw.zookeeper.util.Reference;

public class StampedReference<T> extends AbstractPair<Long, T> implements Reference<T> {

    public static <T> StampedReference<T> of(T value) {
        return new StampedReference<T>(Long.valueOf(0), value);
    }

    public static <T> StampedReference<T> of(Long stamp, T value) {
        return new StampedReference<T>(stamp, value);
    }

    public static class Updater<T> implements Reference<StampedReference<? extends T>> {
        protected final AtomicReference<StampedReference<? extends T>> reference;

        public static <T> Updater<T> newInstance(StampedReference<? extends T> value) {
            return new Updater<T>(value);
        }
        
        protected Updater(StampedReference<? extends T> value) {
            this.reference = new AtomicReference<StampedReference<? extends T>>(checkNotNull(value));
        }
        
        public StampedReference<? extends T> setIfGreater(StampedReference<? extends T> value) {
            StampedReference<? extends T> current = reference.get();
            if (current.stamp() < checkNotNull(value).stamp()) {
                if (! reference.compareAndSet(current, value)) {
                    return setIfGreater(value);
                }
            }
            return current;
        }
        
        @Override
        public StampedReference<? extends T> get() {
            return reference.get();
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
