package edu.uw.zookeeper.net.intravm;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

import edu.uw.zookeeper.common.Actors;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.net.Connection;

/**
 * Queues events when there are no listeners
 */
public class IntraVmPublisher<O> extends Actors.ExecutedPeekingQueuedActor<Object> implements Eventful<Connection.Listener<? super O>> {

    public static <O> IntraVmPublisher<O> defaults(Executor executor, Logger logger) {
        return new IntraVmPublisher<O>(
                ImmutableList.<Connection.Listener<? super O>>of(), 
                executor,
                Queues.<Object>newConcurrentLinkedQueue(),
                logger);
    }

    protected final CopyOnWriteArraySet<Connection.Listener<? super O>> listeners;
    
    protected IntraVmPublisher(
            Iterable<Connection.Listener<? super O>> listeners,
            Executor executor,
            Queue<Object> mailbox,
            Logger logger) {
        super(executor, mailbox, logger);
        this.listeners = Sets.newCopyOnWriteArraySet(listeners);
    }
    
    @Override
    public boolean isReady() {
        return (!listeners.isEmpty() && !mailbox.isEmpty());
    }

    @Override
    public void subscribe(Connection.Listener<? super O> listener) {
        listeners.add(listener);
        run();
    }

    @Override
    public boolean unsubscribe(Connection.Listener<? super O> listener) {
        return listeners.remove(listener);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected boolean apply(Object input) throws Exception {
        Iterator<Connection.Listener<? super O>> itr = listeners.iterator();
        boolean success = itr.hasNext() && mailbox.remove(input);
        if (input instanceof Automaton.Transition) {
            Automaton.Transition<Connection.State> state = (Automaton.Transition<Connection.State>) input;
            while (success && itr.hasNext()) {
                itr.next().handleConnectionState(state);
            }
            if (state.to() == Connection.State.CONNECTION_CLOSED) {
                stop();
            }
        } else {
            O message = (O) input;
            while (success && itr.hasNext()) {
                itr.next().handleConnectionRead(message);
            }
        }
        return success;
    }
    
    @Override
    protected void doStop() {
        mailbox.clear();
        listeners.clear();
    }
}
