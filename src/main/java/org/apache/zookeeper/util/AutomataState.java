package org.apache.zookeeper.util;

public interface AutomataState<T extends AutomataState<T>> {
    boolean isTerminal();
    boolean validTransition(T nextState);
}
