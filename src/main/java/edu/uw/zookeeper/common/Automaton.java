package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.reflect.TypeToken;

public interface Automaton<Q, I> extends Stateful<Q>, Function<I, Optional<Q>> {

    @Event
    public static class Transition<Q> extends AbstractPair<Q, Q> {
    
        public static <Q> Transition<Q> create(Q from, Q to) {
            return new Transition<Q>(from, to);
        }

        @SuppressWarnings("serial")
        private final TypeToken<Q> type = new TypeToken<Q>(getClass()) {};
        
        private Transition(Q from, Q to) {
            super(checkNotNull(from), checkNotNull(to));
        }
        
        public Q from() {
            return first;
        }
        
        public Q to() {
            return second;
        }
        
        public TypeToken<Q> type() {
            return type;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("from", from()).add("to", to()).toString();
        }
    }
}
