package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Function;
import com.google.common.base.Optional;

public class SynchronizedAutomaton<Q,I> implements Automaton<Q,I> {

    public static <Q extends Enum<Q> & Function<I, Optional<Q>>, I> SynchronizedAutomaton<Q,I> create(Class<? extends Q> cls) {
        return wrapper(SimpleAutomaton.create(cls));
    }
    
    public static <Q extends Function<I, Optional<Q>>, I> SynchronizedAutomaton<Q,I> create(
            Q initialState) {
        return wrapper(SimpleAutomaton.create(initialState));
    }
    
    public static <Q,I> SynchronizedAutomaton<Q,I> wrapper(
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
}
