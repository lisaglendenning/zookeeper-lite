package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Optional;

public interface AutomataState<V extends AutomataState<V>> {

    /**
     * Atomically updates an AutomataState while disallowing
     * invalid transitions.
     * 
     * @param <V>
     */
    public static class Reference<V extends AutomataState<V>> implements
            AtomicUpdater<V> {

        public static <V extends AutomataState<V>> Reference<V> create(V state) {
            return new Reference<V>(state);
        }

        private final AtomicReference<V> value;

        protected Reference(V value) {
            this.value = new AtomicReference<V>(checkNotNull(value));
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
            V prevValue = get();
            if (!compareAndSet(prevValue, checkNotNull(value))) {
                return Optional.absent();
            }
            return Optional.of(prevValue);
        }

        @Override
        public boolean compareAndSet(V condition, V value) {
            if (!checkNotNull(condition).validTransition(checkNotNull(value))) {
                return false;
            }
            return this.value.compareAndSet(condition, value);
        }
    }

    boolean isTerminal();

    boolean validTransition(V nextState);
}
