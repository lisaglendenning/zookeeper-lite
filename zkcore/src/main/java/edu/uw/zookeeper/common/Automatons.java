package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.WeakConcurrentSet;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;

public abstract class Automatons {

    public static <Q extends Enum<Q> & Function<I, Optional<Automaton.Transition<Q>>>, I> SimpleAutomaton<Q,I> createSimple(Class<Q> cls) {
        return SimpleAutomaton.create(cls.getEnumConstants()[0]);
    }
    
    public static <Q extends Function<I, Optional<Automaton.Transition<Q>>>, I> SimpleAutomaton<Q,I> createSimple(
            Q initialState) {
        return SimpleAutomaton.create(initialState);
    }

    public static <Q,I,T extends Automaton<Q,I>> SynchronizedAutomaton<Q,I,T> createSynchronized(
            T automaton) {
        return SynchronizedAutomaton.create(automaton);
    }
    
    public static <Q,I,T extends Automaton<Q,I>> SimpleEventfulAutomaton<Q,I,T> createEventful(
            T automaton) {
        return SimpleEventfulAutomaton.create(automaton);
    }

    public static <Q,I,T extends EventfulAutomaton<Q,I>> SynchronizedEventfulAutomaton<Q,I,T> createSynchronizedEventful(
            T automaton) {
        return SynchronizedEventfulAutomaton.create(automaton);
    } 

    /**
     * Not thread-safe
     */
    public static class SimpleAutomaton<Q extends Function<I, Optional<Automaton.Transition<Q>>>, I> implements Automaton<Q,I> {

        public static <Q extends Function<I, Optional<Automaton.Transition<Q>>>, I> SimpleAutomaton<Q,I> create(
                Q initialState) {
            return new SimpleAutomaton<Q,I>(initialState);
        }
        
        private Q state;
     
        protected SimpleAutomaton(Q initialState) {
            this.state = checkNotNull(initialState);
        }

        @Override
        public Q state() {
            return state;
        }
        
        @Override
        public Optional<Automaton.Transition<Q>> apply(I input) {
            Optional<Automaton.Transition<Q>> output = state.apply(input);
            if (output.isPresent()) {
                Q nextState = output.get().to();
                if (! state.equals(nextState)) {
                    state = checkNotNull(nextState);
                }
            }
            return output;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(Automaton.class).addValue(state).toString();
        }
    }
    
    public static class SynchronizedAutomaton<Q,I,T extends Automaton<Q,I>> implements Automaton<Q,I> {

        public static <Q,I,T extends Automaton<Q,I>> SynchronizedAutomaton<Q,I,T> create(
                T automaton) {
            return new SynchronizedAutomaton<Q,I,T>(automaton);
        }
        
        protected final ReadWriteLock lock;
        protected final T automaton;

        protected SynchronizedAutomaton(
                T automaton) {
            this.automaton = checkNotNull(automaton);
            this.lock = new ReentrantReadWriteLock();
        }

        @Override
        public Q state() {
            lock.readLock().lock();
            try {
                return automaton.state();
            } finally {
                lock.readLock().unlock();
            }
        }
        
        @Override
        public Optional<Automaton.Transition<Q>> apply(I input) {
            lock.writeLock().lock();
            try {
                return automaton.apply(input);
            } finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(Automaton.class).addValue(state()).toString();
        }
    }

    public static interface AutomatonListener<Q> {
        void handleAutomatonTransition(Automaton.Transition<Q> transition);
    }
    
    public static interface EventfulAutomaton<Q,I> extends Automaton<Q,I>, Eventful<AutomatonListener<Q>> {}
    
    public static class SimpleEventfulAutomaton<Q,I,T extends Automaton<Q,I>> implements EventfulAutomaton<Q,I> {

        public static <Q,I,T extends Automaton<Q,I>> SimpleEventfulAutomaton<Q,I,T> create(
                T automaton) {
            return new SimpleEventfulAutomaton<Q,I,T>(
                    new WeakConcurrentSet<AutomatonListener<Q>>(), automaton);
        }

        private final IConcurrentSet<AutomatonListener<Q>> listeners;
        private final T automaton;

        protected SimpleEventfulAutomaton(
                IConcurrentSet<AutomatonListener<Q>> listeners,
                T automaton) {
            this.listeners = checkNotNull(listeners);
            this.automaton = checkNotNull(automaton);
        }
        
        @Override
        public Q state() {
            return automaton.state();
        }
        
        /**
         * Not atomic
         */
        @Override
        public Optional<Automaton.Transition<Q>> apply(I input) {
            // even if automaton is synchronized
            // if this function is called concurrently we can end up
            // with events delivered out of order
            Optional<Automaton.Transition<Q>> output = automaton.apply(input);
            if (output.isPresent()) {
                Automaton.Transition<Q> transition = output.get();
                for (AutomatonListener<Q> listener: listeners) {
                    listener.handleAutomatonTransition(transition);
                }
            }
            return output;
        }
        
        @Override
        public void subscribe(AutomatonListener<Q> listener) {
            listeners.add(listener);
        }

        @Override
        public boolean unsubscribe(AutomatonListener<Q> listener) {
            return listeners.remove(listener);
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(Automaton.class).addValue(state()).toString();
        }
    }
    
    public static class SynchronizedEventfulAutomaton<Q,I,T extends EventfulAutomaton<Q,I>> extends SynchronizedAutomaton<Q,I,T> implements EventfulAutomaton<Q,I> {

        public static <Q,I,T extends EventfulAutomaton<Q,I>> SynchronizedEventfulAutomaton<Q,I,T> create(
                T automaton) {
            return new SynchronizedEventfulAutomaton<Q,I,T>(automaton);
        }

        protected SynchronizedEventfulAutomaton(
                T automaton) {
            super(automaton);
        }

        @Override
        public void subscribe(AutomatonListener<Q> listener) {
            automaton.subscribe(listener);
        }

        @Override
        public boolean unsubscribe(AutomatonListener<Q> listener) {
            return automaton.unsubscribe(listener);
        }
    }
}
