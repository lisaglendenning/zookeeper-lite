package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.Logger;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;
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

    public static <Q,I,T extends Automaton<Q,I>> LoggingAutomaton<Q,I,T> createLogging(
            Logger logger, T automaton) {
        return LoggingAutomaton.create(logger, automaton);
    }
    
    public static <Q,I,T extends Automaton<Q,I>> SynchronizedAutomaton<Q,I,T> createSynchronized(
            T automaton) {
        return SynchronizedAutomaton.create(automaton);
    }
    
    public static <Q,I,T extends Automaton<Q,I>> SimpleEventfulAutomaton<Q,I,T> createEventful(
            T automaton) {
        return SimpleEventfulAutomaton.createWeak(automaton);
    }

    public static <Q,I,T extends EventfulAutomaton<Q,I>> SynchronizedEventfulAutomaton<Q,I,T> createSynchronizedEventful(
            T automaton) {
        return SynchronizedEventfulAutomaton.create(automaton);
    } 

    /**
     * Not thread-safe
     */
    public static final class SimpleAutomaton<Q extends Function<I, Optional<Automaton.Transition<Q>>>, I> implements Automaton<Q,I> {

        public static <Q extends Function<I, Optional<Automaton.Transition<Q>>>, I> SimpleAutomaton<Q,I> create(
                Q initialState) {
            return new SimpleAutomaton<Q,I>(checkNotNull(initialState));
        }
        
        private Q state;
     
        protected SimpleAutomaton(Q initialState) {
            this.state = initialState;
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

    public static abstract class ForwardingAutomaton<Q,I,T extends Automaton<Q,? super I>> implements Automaton<Q,I> {

        protected ForwardingAutomaton() {}

        @Override
        public Q state() {
            return delegate().state();
        }
        
        @Override
        public Optional<Automaton.Transition<Q>> apply(I input) {
            return delegate().apply(input);
        }
        
        @Override
        public String toString() {
            return delegate().toString();
        }
        
        protected abstract T delegate();
    }
    
    public static class LoggingAutomaton<Q,I,T extends Automaton<Q,I>> extends ForwardingAutomaton<Q,I,T> {

        public static <Q,I,T extends Automaton<Q,I>> LoggingAutomaton<Q,I,T> create(
                Logger logger, T automaton) {
            return new LoggingAutomaton<Q,I,T>(checkNotNull(logger), checkNotNull(automaton));
        }
        
        protected final Logger logger;
        protected final T automaton;
     
        protected LoggingAutomaton(Logger logger, T automaton) {
            this.logger = logger;
            this.automaton = automaton;
        }

        @Override
        public Optional<Automaton.Transition<Q>> apply(I input) {
            Optional<Automaton.Transition<Q>> output = super.apply(input);
            if (output.isPresent()) {
                log(output.get());
            }
            return output;
        }
        
        protected void log(Automaton.Transition<Q> transition) {
            logger.debug("{} ({})", transition, this);
        }
        
        @Override
        protected T delegate() {
            return automaton;
        }
    }
    
    public static class SynchronizedAutomaton<Q,I,T extends Automaton<Q,I>> extends ForwardingAutomaton<Q,I,T> {

        public static <Q,I,T extends Automaton<Q,I>> SynchronizedAutomaton<Q,I,T> create(
                T automaton) {
            return new SynchronizedAutomaton<Q,I,T>(checkNotNull(automaton), new ReentrantReadWriteLock());
        }
        
        protected final ReadWriteLock lock;
        protected final T automaton;

        protected SynchronizedAutomaton(
                T automaton,
                ReadWriteLock lock) {
            this.automaton = automaton;
            this.lock = lock;
        }
        
        public ReadWriteLock lock() {
            return lock;
        }

        @Override
        public Q state() {
            lock.readLock().lock();
            try {
                return super.state();
            } finally {
                lock.readLock().unlock();
            }
        }
        
        @Override
        public Optional<Automaton.Transition<Q>> apply(I input) {
            lock.writeLock().lock();
            try {
                return super.apply(input);
            } finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public String toString() {
            lock.readLock().lock();            
            try {
                return super.toString();
            } finally {
                lock.readLock().unlock();
            }
        }

        @Override
        protected T delegate() {
            return automaton;
        }
    }

    public static interface AutomatonListener<Q> {
        void handleAutomatonTransition(Automaton.Transition<Q> transition);
    }
    
    public static interface EventfulAutomaton<Q,I> extends Automaton<Q,I>, Eventful<AutomatonListener<Q>> {}
    
    public static final class SimpleEventfulAutomaton<Q,I,T extends Automaton<Q,I>> extends ForwardingAutomaton<Q,I,T> implements EventfulAutomaton<Q,I> {

        public static <Q,I,T extends Automaton<Q,I>> SimpleEventfulAutomaton<Q,I,T> createWeak(
                T automaton) {
            return new SimpleEventfulAutomaton<Q,I,T>(
                    new WeakConcurrentSet<AutomatonListener<Q>>(), checkNotNull(automaton));
        }
        
        public static <Q,I,T extends Automaton<Q,I>> SimpleEventfulAutomaton<Q,I,T> createStrong(
                T automaton) {
            return new SimpleEventfulAutomaton<Q,I,T>(
                    new StrongConcurrentSet<AutomatonListener<Q>>(), checkNotNull(automaton));
        }

        private final IConcurrentSet<AutomatonListener<Q>> listeners;
        private final T automaton;

        protected SimpleEventfulAutomaton(
                IConcurrentSet<AutomatonListener<Q>> listeners,
                T automaton) {
            this.listeners = listeners;
            this.automaton = automaton;
        }
        
        /**
         * Not atomic
         */
        @Override
        public Optional<Automaton.Transition<Q>> apply(I input) {
            // even if automaton is synchronized
            // if this function is called concurrently we can end up
            // with events delivered out of order
            Optional<Automaton.Transition<Q>> output = super.apply(input);
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
        protected T delegate() {
            return automaton;
        }
    }
    
    public static final class SynchronizedEventfulAutomaton<Q,I,T extends EventfulAutomaton<Q,I>> extends SynchronizedAutomaton<Q,I,T> implements EventfulAutomaton<Q,I> {

        public static <Q,I,T extends EventfulAutomaton<Q,I>> SynchronizedEventfulAutomaton<Q,I,T> create(
                T automaton) {
            return new SynchronizedEventfulAutomaton<Q,I,T>(checkNotNull(automaton), new ReentrantReadWriteLock());
        }

        protected SynchronizedEventfulAutomaton(
                T automaton,
                ReadWriteLock lock) {
            super(automaton, lock);
        }

        @Override
        public void subscribe(AutomatonListener<Q> listener) {
            delegate().subscribe(listener);
        }

        @Override
        public boolean unsubscribe(AutomatonListener<Q> listener) {
            return delegate().unsubscribe(listener);
        }
    }
}
