package edu.uw.zookeeper.util;

import com.google.common.base.Function;
import com.google.common.base.Optional;

public interface Automaton<Q, I> extends Stateful<Q>, Function<I, Optional<Q>> {
}
