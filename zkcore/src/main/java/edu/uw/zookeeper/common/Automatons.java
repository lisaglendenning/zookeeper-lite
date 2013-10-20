package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.engio.mbassy.PubSubSupport;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;

public abstract class Automatons {

    public static <Q extends Enum<Q> & Function<I, Optional<Q>>, I> SimpleAutomaton<Q,I> createSimple(Class<Q> cls) {
        return SimpleAutomaton.create(cls.getEnumConstants()[0]);
    }
    
    public static <Q extends Function<I, Optional<Q>>, I> SimpleAutomaton<Q,I> createSimple(
            Q initialState) {
        return SimpleAutomaton.create(initialState);
    }

    public static <Q,I> SynchronizedAutomaton<Q,I> createSynchronized(
            Automaton<Q,I> automaton) {
        return SynchronizedAutomaton.create(automaton);
    }
    
    public static <Q,I> EventfulAutomaton<Q,I> createEventful(
            PubSubSupport<Object> publisher,
            Automaton<Q,I> automaton) {
        return EventfulAutomaton.create(publisher, automaton);
    }

    public static <Q,I> SynchronizedEventfulAutomaton<Q,I> createSynchronizedEventful(
            PubSubSupport<Object> publisher,
            Automaton<Q,I> automaton) {
        return SynchronizedEventfulAutomaton.create(publisher, automaton);
    } 

    public static class SimpleAutomaton<Q extends Function<I, Optional<Q>>, I> implements Automaton<Q,I> {

        public static <Q extends Function<I, Optional<Q>>, I> SimpleAutomaton<Q,I> create(
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
        public Optional<Q> apply(I input) {
            Optional<Q> nextState = state.apply(input);
            if (nextState.isPresent() && ! state.equals(nextState.get())) {
                state = nextState.get();
            }
            return nextState;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(Automaton.class).add("state", state()).toString();
        }
    }
    
    public static class SynchronizedAutomaton<Q,I> implements Automaton<Q,I> {

        public static <Q,I> SynchronizedAutomaton<Q,I> create(
                Automaton<Q,I> automaton) {
            return new SynchronizedAutomaton<Q,I>(automaton);
        }
        
        private final ReadWriteLock lock;
        private final Automaton<Q,I> automaton;

        protected SynchronizedAutomaton(
                Automaton<Q,I> automaton) {
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
        public Optional<Q> apply(I input) {
            lock.writeLock().lock();
            try {
                return automaton.apply(input);
            } finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(Automaton.class).add("state", state()).toString();
        }
    }
    
    public static class EventfulAutomaton<Q,I> implements Automaton<Q,I>, PubSubSupport<Object> {

        public static <Q,I> EventfulAutomaton<Q,I> create(
                PubSubSupport<Object> publisher,
                Automaton<Q,I> automaton) {
            return new EventfulAutomaton<Q,I>(publisher, automaton);
        }

        private final PubSubSupport<Object> publisher;
        private final Automaton<Q,I> automaton;

        protected EventfulAutomaton(
                PubSubSupport<Object> publisher,
                Automaton<Q,I> automaton) {
            this.publisher = checkNotNull(publisher);
            this.automaton = checkNotNull(automaton);
        }
        
        protected Automaton<Q,I> automaton() {
            return automaton;
        }

        @Override
        public Q state() {
            return automaton().state();
        }
        
        @Override
        public Optional<Q> apply(I input) {
            // Warning: not atomic!
            Q curState = automaton().state();
            Optional<Q> nextState = automaton().apply(input);
            if (nextState.isPresent() && ! curState.equals(nextState.get())) {
                publish(Automaton.Transition.create(curState, nextState.get()));
            }
            return nextState;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(Automaton.class).add("state", state()).toString();
        }

        @Override
        public void subscribe(Object listener) {
            publisher.subscribe(listener);
        }

        @Override
        public boolean unsubscribe(Object listener) {
            return publisher.unsubscribe(listener);
        }

        @Override
        public void publish(Object message) {
            publisher.publish(message);
        }    
    }
    
    public static class SynchronizedEventfulAutomaton<Q,I> extends EventfulAutomaton<Q,I> {

        public static <Q,I> SynchronizedEventfulAutomaton<Q,I> create(
                PubSubSupport<Object> publisher,
                Automaton<Q,I> automaton) {
            return new SynchronizedEventfulAutomaton<Q,I>(publisher, automaton);
        }

        private final ReadWriteLock lock;
        
        protected SynchronizedEventfulAutomaton(
                PubSubSupport<Object> publisher,
                Automaton<Q,I> automaton) {
            super(publisher, automaton);
            this.lock = new ReentrantReadWriteLock();
        }

        @Override
        public Q state() {
            lock.readLock().lock();
            try {
                return automaton().state();
            } finally {
                lock.readLock().unlock();
            }
        }
        
        @Override
        public Optional<Q> apply(I input) {
            Q curState;
            Optional<Q> nextState;
            lock.writeLock().lock();
            try {
                curState = automaton().state();
                nextState = automaton().apply(input);
            } finally {
                lock.writeLock().unlock();
            }
            if (nextState.isPresent() && ! curState.equals(nextState.get())) {
                publish(Automaton.Transition.create(curState, nextState.get()));
            }
            return nextState;
        }
    }
}
