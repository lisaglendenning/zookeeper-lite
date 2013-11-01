package edu.uw.zookeeper.net.intravm;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.WeakConcurrentSet;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Monitor;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.net.Connection;

public class IntraVmPublisher<O> implements Eventful<Connection.Listener<? super O>>, Connection.Listener<O> {

    public static <O> IntraVmPublisher<O> weakSubscribers() {
        return new IntraVmPublisher<O>(
                new WeakConcurrentSet<Connection.Listener<? super O>>());
    }

    // don't post events until someone subscribes
    private final Monitor monitor;
    private final Monitor.Guard isSubscribed;
    private boolean subscribed;
    private final IConcurrentSet<Connection.Listener<? super O>> listeners;
    
    protected IntraVmPublisher(
            IConcurrentSet<Connection.Listener<? super O>> listeners) {
        this.listeners = listeners;
        this.subscribed = listeners.size() > 0;
        this.monitor = new Monitor();
        this.isSubscribed = new Monitor.Guard(monitor) {
            public boolean isSatisfied() {
                return subscribed;
            }
        };
    }
    
    public boolean isSubscribed() {
        monitor.enter();
        try {
            return subscribed;
        } finally {
            monitor.leave();
        }
    }

    @Override
    public void subscribe(Connection.Listener<? super O> listener) {
        monitor.enter();
        try {
            listeners.add(listener);
            if (! subscribed) {
                subscribed = true;
            }
        } finally {
            monitor.leave();
        }
    }

    @Override
    public boolean unsubscribe(Connection.Listener<? super O> listener) {
        // we don't set subscribe to false once it's true
        return listeners.remove(listener);
    }

    @Override
    public void handleConnectionState(Automaton.Transition<Connection.State> state) {
        try {
            monitor.enterWhen(isSubscribed);
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
        try {
            for (Connection.Listener<? super O> listener : listeners) {
                listener.handleConnectionState(state);
            }
        } finally {
            monitor.leave();
        }
    }

    @Override
    public void handleConnectionRead(O message) {
        try {
            monitor.enterWhen(isSubscribed);
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
        try {
            for (Connection.Listener<? super O> listener : listeners) {
                listener.handleConnectionRead(message);
            }
        } finally {
            monitor.leave();
        }
    }
}
