package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Function;
import com.google.common.base.Optional;

public class SimpleAutomaton<Q extends Function<I, Optional<Q>>, I> implements Automaton<Q,I> {

    public static <Q extends Enum<Q> & Function<I, Optional<Q>>, I> SimpleAutomaton<Q,I> create(Class<? extends Q> cls) {
        return new SimpleAutomaton<Q,I>(cls.getEnumConstants()[0]);
    }
    
    public static <Q extends Function<I, Optional<Q>>, I> SimpleAutomaton<Q,I> create(
            Q initialState) {
        return new SimpleAutomaton<Q,I>(initialState);
    }
    
    private Q state;
 
    private SimpleAutomaton(Q initialState) {
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
}
