package edu.uw.zookeeper.util;

import com.google.common.base.Optional;

/**
 * Atomic updater for a value of type <code>V</code>.
 * 
 * Interface inspired by <code>AtomicReference</code>, but
 * exposes set operations that can fail.
 * 
 * @param <V>
 */
public interface AtomicUpdater<V> {

    /**
     * Atomically read the value.
     * 
     * @return the value
     */
    V get();

    /**
     * Atomically set the value.
     * 
     * @param value the value to set
     * @return whether the value was successfully set
     */
    boolean set(V value);

    /**
     * Atomically set the value and return the previous value.
     * 
     * @param value the value to set
     * @return Optional.absent() if the set operation failed, otherwise Optional.of(previous value)
     */
    Optional<V> getAndSet(V value);

    /**
     * Atomically sets the value to <code>value</code> if it equals <code>condition</code>.
     * 
     * @param condition the conditional value
     * @param value the value to set
     * @return whether the value was successfully set
     */
    boolean compareAndSet(V condition, V value);
}
