package org.apache.zookeeper.util;

public interface AutomataState<T extends AutomataState<T>> {
    T initial();
    boolean isTerminal();
    boolean validTransition(T nextState);
}
