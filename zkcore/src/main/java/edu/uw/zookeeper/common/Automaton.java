package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Function;
import com.google.common.base.Optional;

public interface Automaton<Q, I> extends Stateful<Q>, Function<I, Optional<Automaton.Transition<Q>>> {

    public static class Transition<Q> extends AbstractPair<Q, Q> {
    
        public static <Q> Transition<Q> create(Q from, Q to) {
            return new Transition<Q>(from, to);
        }
        
        private Transition(Q from, Q to) {
            super(checkNotNull(from), checkNotNull(to));
        }
        
        public Q from() {
            return first;
        }
        
        public Q to() {
            return second;
        }
    }
}
