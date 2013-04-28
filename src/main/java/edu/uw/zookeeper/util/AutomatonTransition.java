package edu.uw.zookeeper.util;

import com.google.common.base.Objects;

public class AutomatonTransition<Q> extends AbstractPair<Q, Q> {

    public static <Q> AutomatonTransition<Q> create(Q from, Q to) {
        return new AutomatonTransition<Q>(from, to);
    }
    
    private AutomatonTransition(Q from, Q to) {
        super(from, to);
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

