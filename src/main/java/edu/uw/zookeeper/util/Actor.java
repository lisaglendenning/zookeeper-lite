package edu.uw.zookeeper.util;

public interface Actor<I> extends Runnable, Stateful<Actor.State> {
    
    public static enum State {
        WAITING, SCHEDULED, RUNNING, TERMINATED;
    }
    
    void send(I message);
    
    boolean stop();

    boolean schedule();
}
