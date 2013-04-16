package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.*;

import com.google.common.base.Optional;

/**
 * Broadcasts value changes as events.
 * 
 * @param <V>
 */
public class EventfulAtomicUpdater<V> extends ForwardingEventful implements
        Eventful, AtomicUpdater<V> {

    public static <V> EventfulAtomicUpdater<V> create(Eventful eventful,
            AtomicUpdater<V> value) {
        return new EventfulAtomicUpdater<V>(eventful, value);
    }

    private final AtomicUpdater<V> value;

    protected EventfulAtomicUpdater(Eventful eventful, AtomicUpdater<V> value) {
        super(eventful);
        this.value = checkNotNull(value);
    }

    @Override
    public V get() {
        return value.get();
    }

    @Override
    public boolean set(V value) {
        return (getAndSet(value).isPresent());
    }

    @Override
    public Optional<V> getAndSet(V value) {
        Optional<V> prevValue = this.value.getAndSet(value);
        if (prevValue.isPresent() && (prevValue.get() != value)) {
            post(value);
        }
        return prevValue;
    }

    @Override
    public boolean compareAndSet(V condition, V value) {
        boolean updated = this.value.compareAndSet(condition, value);
        if (updated && (condition != value)) {
            post(value);
        }
        return updated;
    }
}
