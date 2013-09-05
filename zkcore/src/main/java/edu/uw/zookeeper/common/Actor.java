package edu.uw.zookeeper.common;

public interface Actor<T> extends Runnable, Stateful<Actor.State> {
    
    public static enum State {
        WAITING, SCHEDULED, RUNNING, TERMINATED;
    }
    
    boolean send(T message);
    
    boolean stop();
}
