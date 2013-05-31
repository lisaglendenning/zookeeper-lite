package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;

public interface Automaton<Q, I> extends Stateful<Q>, Function<I, Optional<Q>> {

    @Event
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
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("from", from()).add("to", to()).toString();
        }
    }
}
