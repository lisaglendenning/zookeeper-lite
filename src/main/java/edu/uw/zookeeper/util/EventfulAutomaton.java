package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Function;
import com.google.common.base.Optional;

public class EventfulAutomaton<Q,I> extends ForwardingEventful implements Automaton<Q,I>, Eventful {

    public static <Q extends Enum<Q> & Function<I, Optional<Q>>, I> EventfulAutomaton<Q,I> create(
            Publisher publisher,
            Class<? extends Q> cls) {
        return wrap(publisher, SimpleAutomaton.create(cls));
    }
    
    public static <Q extends Function<I, Optional<Q>>, I> EventfulAutomaton<Q,I> create(
            Publisher publisher,
            Q initialState) {
        return wrap(publisher, SimpleAutomaton.create(initialState));
    }
    
    public static <Q,I> EventfulAutomaton<Q,I> wrap(
            Publisher publisher,
            Automaton<Q,I> automaton) {
        return new EventfulAutomaton<Q,I>(publisher, automaton);
    }

    public static <Q extends Enum<Q> & Function<I, Optional<Q>>, I> SynchronizedEventfulAutomaton<Q,I> createSynchronized(
            Publisher publisher,
            Class<? extends Q> cls) {
        return wrapSynchronized(publisher, SimpleAutomaton.create(cls));
    }
    
    public static <Q extends Function<I, Optional<Q>>, I> SynchronizedEventfulAutomaton<Q,I> createSynchronized(
            Publisher publisher,
            Q initialState) {
        return wrapSynchronized(publisher, SimpleAutomaton.create(initialState));
    }
    
    public static <Q,I> SynchronizedEventfulAutomaton<Q,I> wrapSynchronized(
            Publisher publisher,
            Automaton<Q,I> automaton) {
        return SynchronizedEventfulAutomaton.wrap(publisher, automaton);
    } 

    private final Automaton<Q,I> automaton;

    protected EventfulAutomaton(
            Publisher publisher,
            Automaton<Q,I> automaton) {
        super(publisher);
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
            post(AutomatonTransition.create(curState, nextState.get()));
        }
        return nextState;
    }
    
    public static class SynchronizedEventfulAutomaton<Q,I> extends EventfulAutomaton<Q,I> {

        public static <Q,I> SynchronizedEventfulAutomaton<Q,I> wrap(
                Publisher publisher,
                Automaton<Q,I> automaton) {
            return new SynchronizedEventfulAutomaton<Q,I>(publisher, automaton);
        }

        private final ReadWriteLock lock;
        
        private SynchronizedEventfulAutomaton(
                Publisher publisher,
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
                post(AutomatonTransition.create(curState, nextState.get()));
            }
            return nextState;
        }
    }
}
